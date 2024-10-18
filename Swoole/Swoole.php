<?php
namespace Tanbolt\Websocket\Swoole;

use swoole_table;
use swoole_process;
use swoole_http_request;
use swoole_http_response;
use swoole_websocket_frame;
use swoole_websocket_server;
use Tanbolt\Websocket\Event;
use Tanbolt\Websocket\Websocket;
use Tanbolt\Websocket\EngineInterface;

/**
 * Class Swoole
 * @package Tanbolt\Websocket\Swoole
 */
class Swoole implements EngineInterface
{
    /**
     * @var bool
     */
    protected static $debug;

    /**
     * swoole webSocket server 对象
     * @var swoole_websocket_server
     */
    protected $swooleServer;

    /**
     * swoole 独立进程对象 (内存存储连接用户信息)
     * @var BusServer
     */
    protected $swooleProcess;

    /**
     * 架在 client 与 server 之间的上下文对象
     * @var BusContext
     */
    protected $context;

    /**
     * @var array
     */
    protected $options = [];

    /**
     * 是否开启了 Https
     * @var bool
     */
    protected $httpsOn = null;

    /**
     * 客户端真实 ip header key 值
     * @var string
     */
    protected $realIp = null;

    /**
     * Server 内网地址, swoole request server 不含这一项, 保持与 php-fpm 相同, 手动获取
     * @var string
     */
    protected $serverAddr = null;

    /**
     * 监听端口
     * @var int
     */
    protected $serverPort = null;

    /**
     * 共享内存, 用来存其他节点服务器的 fd
     * @var swoole_table
     */
    protected $nodeTable;

    /**
     * @param $msg
     */
    public static function debug($msg)
    {
        if (static::$debug) {
            echo $msg."\r\n";
        }
    }

    /**
     * 打包 $area, $node, $session => 全局唯一 clientId
     * polling: sessionHash-nodeHash[-areaHash]
     * webSocket: sessionId/nodeId/areaId
     * @param $area
     * @param $node
     * @param $session
     * @param bool $polling
     * @return string
     */
    public static function packClientId($area, $node, $session, $polling = false)
    {
        if ($polling) {
            return 'p'.$session.'-'.$node.(strlen($area) ? '-'.$area : '');
        }
        if (strlen($area)) {
            return 'w'.base_convert(pack('Nnn', $session, $node, $area), 2, 36);
        }
        return 'w'.bin2hex(pack('Nn', $session, $node));
    }

    /**
     * 全局唯一 clientId => 解包 $area, $node, $session
     * @param $clientId
     * @return array|false
     */
    public static function unpackClientId($clientId)
    {
        $polling = substr($clientId, 0, 1);
        $polling = $polling === 'w' ? false : ($polling === 'p' ? true : null);
        if ($polling === null) {
            return false;
        }
        $clientId = substr($clientId, 1);
        if ($polling) {
            $arr = explode('-', $clientId);
            if (($count = count($arr)) < 2) {
                return false;
            }
            $session = $arr[0];
            $node = $arr[1];
            $area = $arr[2] ?? null;
            return compact('polling', 'session', 'node', 'area');
        }
        $clientId = hex2bin($clientId);
        $clientLen = strlen($clientId);
        if ($clientLen != 6 && $clientLen !== 8) {
            return false;
        }
        set_error_handler(function(){});
        $arr = $clientLen === 8 ? 'Nsession/nnode/narea' : 'Nsession/nnode';
        $arr = unpack($arr, $clientId);
        restore_error_handler();
        $count = count($arr);
        if ($clientLen === 8 &&  $count === 3) {
            $arr['polling'] = $polling;
            return $arr;
        }
        if ($count === 2) {
            $arr['area'] = null;
            $arr['polling'] = $polling;
            return $arr;
        }
        return false;
    }

    /**
     * Swoole constructor.
     * @param $host
     * @param $port
     * @param array $options
     */
    public function __construct($host, $port, array $options = [])
    {
        $this->options = $options;

        // debug
        if (isset($options['debug'])) {
            static::$debug = (bool) $options['debug'];
            unset($options['debug']);
        }

        // 是否 https
        if (array_key_exists('proxy_ssl', $options)) {
            $this->httpsOn = $options['proxy_ssl'] === null ? null : (bool) $options['proxy_ssl'];
            unset($options['proxy_ssl']);
        }

        // 真实 ip header key
        if (!array_key_exists('real_ip', $options)) {
            $this->realIp = 'x-forwarded-for';
        } else {
            if ($options['real_ip'] === false) {
                $this->realIp = false;
            } elseif (empty($options['real_ip'])) {
                $this->realIp = 'x-forwarded-for';
            } else {
                $this->realIp = strtolower($options['real_ip']);
            }
            unset($options['real_ip']);
        }

        // 当前节点内网地址
        $this->serverAddr = Websocket::getServerAddr();
        $this->serverPort = (int) $port;

        // 注册节点内网地址
        $register = '';
        if (isset($options['register'])) {
            $register = $options['register'];
            unset($options['register']);
        }

        // 创建共享内存 table (若禁用 polling 共享内存只存储内网节点记录, size 可尽量小)
        $this->nodeTable = new swoole_table($options['disable_polling'] ? 1024 : 16384);
        $this->nodeTable->column('m', swoole_table::TYPE_INT, 1);
        $this->nodeTable->create();
        unset($options['disable_polling'], $options['ping_interval']);

        // 创建 webSocket 服务端 / 独立内存进程
        $node = $this->serverAddr.':'.$port;
        $this->swooleServer = new swoole_websocket_server($host, $port);
        $this->swooleProcess = new swoole_process(function(swoole_process $process) use ($node, $register) {
            // 独立进程 code 开始, 清空初始共享变量, 反正也没法用, 清掉省点内存
            $this->options = $this->swooleProcess = $this->realIp = $this->httpsOn = $this->serverAddr = null;
            new BusServer($this->swooleServer, $process, $node, $register);
        });
        $this->swooleServer->addProcess($this->swooleProcess);
        $this->iniOptions($options)->initEvents();
    }

    /**
     * 获取初始设置
     * @param null $key
     * @return array|mixed|null
     */
    public function getOptions($key = null)
    {
        if (empty($key)) {
            return $this->options;
        }
        return array_key_exists($key, $this->options) ? $this->options[$key] : null;
    }

    /**
     * 设置初始参数 (dispatch_mode 必须为 2)
     * @param array $options
     * @return $this
     */
    protected function iniOptions(array $options = [])
    {


        $this->swooleServer->set([
            'worker_num' => 4,
            'dispatch_mode' => 2
        ]);
        return $this;
    }

    /**
     * 绑定监听事件
     * @return $this
     */
    protected function initEvents()
    {
        $this->swooleServer->on('workerStart', [$this, 'onWorkerStart']);
        $this->swooleServer->on('workerStop', [$this, 'onWorkerStop']);
        $this->swooleServer->on('pipeMessage', [$this, 'onPipeMessage']);
        $this->swooleServer->on('connect', [$this, 'onConnect']);
        $this->swooleServer->on('request', [$this, 'onRequest']);
        $this->swooleServer->on('open', [$this, 'onOpen']);
        $this->swooleServer->on('message', [$this, 'onMessage']);
        $this->swooleServer->on('close', [$this, 'onClose']);
        return $this;
    }

    /**
     * 获取 swoole webSocket server 对象
     * @return swoole_websocket_server
     */
    public function swooleServer()
    {
        return $this->swooleServer;
    }

    /**
     * worker 进程启动回调
     * @param swoole_websocket_server $server
     * @param $workerId
     * @return $this
     */
    public function onWorkerStart(swoole_websocket_server $server, $workerId)
    {
        $this->context = new BusContext($this, $this->swooleProcess,$this->serverAddr.':'.$this->serverPort);
        call_user_func(Event::$onWorkerStart, $workerId);
        return $this;
    }

    /**
     * worker 进程停止回调
     * @param swoole_websocket_server $server
     * @param $workerId
     * @return $this
     */
    public function onWorkerStop(swoole_websocket_server $server, $workerId)
    {
        call_user_func(Event::$onWorkerStop, $workerId);
        return $this;
    }

    /**
     * worker 收到 process pipe message 回调
     * @param swoole_websocket_server $server
     * @param $processId
     * @param $message
     */
    public function onPipeMessage(swoole_websocket_server $server, $processId, $message)
    {
        $this->context->onReceiveMessage($message);
    }

    /**
     * chrome 等浏览器可能是使用了 TCP 复用技术, 导致 close 回调触发的 fd 可能既不是 request, 也不是 webSocket
     * 而仅仅是 tcp 关闭, 对于这种情况的客户端关闭, onClose 回调是没必要处理的
     * @param swoole_websocket_server $server
     * @param $fd
     * @return $this
     */
    public function onConnect(swoole_websocket_server $server, $fd)
    {
        $this->nodeTable->set($fd, ['m' => 0]);
        return $this;
    }

    /**
     * Http 请求回调
     * @param swoole_http_request $request
     * @param swoole_http_response $response
     * @return $this
     */
    public function onRequest(swoole_http_request $request, swoole_http_response $response)
    {
        if ($this->isNodeRequest($request)) {
            $trick = 1;
        } elseif ($this->getOptions('disable_polling')) {
            $trick = 2;
        } else {
            static::debug('[swoole]user request:'.$request->fd);
            $trick = $request->server['request_method'] === 'GET' && isset($request->get['polling'])
                && $request->get['polling'] === 'polling' ? 4 : 3;
        }
        // http request 也会触发 onClose 事件, 打点方便 onClose 进行甄别, 不同的 Http 请求打点不同的值
        // 4:eventSource, 3:normal, 2:disabled, 1:node
        $this->nodeTable->set($request->fd, ['m' => $trick]);
        if ($trick > 2) {
            $this->onPollingRequest($request, $response);
        } elseif ($trick > 1) {
            $response->status(403);
            $response->end();
        } else {
            $this->onNodeRequest($request, $response);
        }
        return $this;
    }

    /**
     * 通过 SERVER 中的来源 IP 和 header 判断是否内网节点请求
     * @param swoole_http_request $request
     * @return bool
     */
    protected function isNodeRequest(swoole_http_request $request)
    {
        return isset($request->server['path_info']) && $request->server['path_info'] === '/'
            && isset($request->server['remote_addr'])
            && !filter_var($request->server['remote_addr'], FILTER_VALIDATE_IP, FILTER_FLAG_NO_PRIV_RANGE)
            && isset($request->header['x-server-client']) && $request->header['x-server-client'] === 'client';
    }

    /**
     * 内网节点 http 通信
     * @param swoole_http_request $request
     * @param swoole_http_response $response
     * @return bool
     */
    protected function onNodeRequest(swoole_http_request $request, swoole_http_response $response)
    {
        // 当前节点作为注册节点 -> 收到了 新上节点通知 -> 注册新节点 -> 返回所有已注册节点
        if (isset($request->header['x-register'])) {
            static::debug('[swoole]node request - response all nodes:'.$request->fd);
            $nodes = $this->context->registerNode($request->header['x-register']);
            $response->end(json_encode($nodes));
            return true;
        }
        // 当前节点作为普通节点 -> 收到了节点变化通知 -> 更新节点 -> 返回 ok
        if (isset($request->header['x-nodes'])) {
            static::debug('[swoole]node request - add one node:'.$request->fd);
            $response->end('ok');
            $this->context->addNode(json_decode($request->header['x-nodes'], true));
            return true;
        }
        return false;
    }

    /**
     * 客户端 http polling 请求
     * @param swoole_http_request $request
     * @param swoole_http_response $response
     * @return $this
     */
    protected function onPollingRequest(swoole_http_request $request, swoole_http_response $response)
    {
        list($header, $server) = $this->formatRequestHeaderServer($request);
        $request->header = $header;
        $request->server = $server;
        $this->context->resolvePollingRequest($request, $response);
        return $this;
    }

    /**
     * webSocket 客户端握手成功回调
     * @param swoole_websocket_server $server
     * @param swoole_http_request $request
     * @return $this
     */
    public function onOpen(swoole_websocket_server $server, swoole_http_request $request)
    {
        // 需要处理 onClose 回调, 删除 memory table 的缓存
        $this->nodeTable->del($request->fd);
        // 如果客户端是集群内节点, 记录下 分布式节点客户端的 fd, 方便 onMessage 时特殊处理
        if ($this->isNodeRequest($request)) {
            static::debug('[swoole]node open:'.$request->fd);
            $this->nodeTable->set('s'.$request->fd, ['m' => 1]);
            return $this;
        }
        // 处理普通客户端连接
        static::debug('[swoole]user open:'.$request->fd);
        list($header, $server) = $this->formatRequestHeaderServer($request);
        $sessionId = $request->fd;
        $query = $request->get;
        $cookie = $request->cookie;
        $request = compact('query', 'header', 'server', 'cookie');

        // 记录当前连接fd, 若自定义 open 回调抛出异常, 不触发自定义 close 回调 (理解为 open 失败, 自然也没有 close 这一说了)
        $this->nodeTable->set('e'.$sessionId, ['m' => 1]);
        if (true === $this->context->openClient($sessionId, $request)) {
            $this->nodeTable->del('e'.$sessionId);
        }
        return $this;
    }

    /**
     * 格式化 request header server, 设置真实IP
     * @param swoole_http_request $request
     * @return array
     */
    protected function formatRequestHeaderServer(swoole_http_request $request)
    {
        $header = $request->header;
        $server = $request->server;
        // 代理转发, 获取真实 IP
        if (!empty($this->realIp) && isset($header[$this->realIp]) && !empty($header[$this->realIp])) {
            $allowIps = [];
            foreach (explode(',', $header[$this->realIp]) as $ip) {
                $ip = trim($ip);
                if (filter_var($ip, FILTER_VALIDATE_IP)) {
                    $allowIps[] = $ip;
                }
            }
            unset($header[$this->realIp]);
            if (count($allowIps)) {
                $clientIp = array_pop($allowIps);
                $server['REMOTE_ADDR'] = $clientIp;
                if (count($allowIps)) {
                    $header['x-forwarded-for'] = join(',',  $allowIps);
                }
            }
        }
        // 代理转发, 设置是否为 https
        if ($this->httpsOn !== null) {
            if ($this->httpsOn) {
                $server['HTTPS'] = 'on';
            } elseif (isset($server['HTTPS'])) {
                unset($server['HTTPS']);
            }
        }
        // 设置 Server_addr
        $server['SERVER_ADDR'] = $this->serverAddr;

        $header = array_combine(array_map(function($key) {
            return join('-', array_map('ucfirst', explode('-', strtolower($key))));
        }, array_keys($header)), $header);
        $server = array_combine(array_map(function($key) {
            return strtoupper($key);
        }, array_keys($server)), $server);
        return [$header, $server];
    }

    /**
     * webSocket 客户端发来消息
     * @param swoole_websocket_server $server
     * @param swoole_websocket_frame $frame
     * @return $this
     */
    public function onMessage(swoole_websocket_server $server, swoole_websocket_frame $frame)
    {
        if ($this->nodeTable->exist('s'.$frame->fd)) {
            // 客户端是集群内节点
            static::debug('[swoole]receive node message:'.$frame->fd);
            $this->context->resolveNodeMessage($frame);
        } else {
            // 普通客户端 (心跳就不 debug 了)
            if ('1' !== $frame->data) {
                static::debug('[swoole]receive user message:'.$frame->fd);
            }
            if (true === $this->context->resolveWebSocketMessage($frame)) {
                $this->nodeTable->del('e'.$frame->fd);
            }
        }
        return $this;
    }

    /**
     * webSocket/httpRequest 连接断开
     * swoole 目前为止,不支持 close code reason (当前版本 4.0.1)
     * 可以用 TCP 接收数据自己进行解包, 但这会带来另外一个问题,
     * worker 收到的包不是连续的, 受 dispatch_mode 影响, 同一个完整数据可能会分成不同的片段发送给不同 worker
     * 所以这条路不好走, 只能等待 swoole 支持了
     * @param swoole_websocket_server $server
     * @param $fd
     * @return $this
     */
    public function onClose(swoole_websocket_server $server, $fd)
    {
        // 集群节点 webSocket 客户端断开
        if ($this->nodeTable->exist('s'.$fd)) {
            static::debug('[swoole]node client close:'.$fd);
            $this->nodeTable->del('s'.$fd);
            return $this;
        }
        // http 请求结束
        $trick = $this->nodeTable->get($fd, 'm');
        if ($trick !== false) {
            $this->nodeTable->del($fd);
            if ($trick === 4) {
                // eventSource 连接断开
                static::debug('[swoole]pooling client close:'.$fd);
                $this->context->resolvePollingClose($fd);
            }
            return $this;
        }
        if ($error = $this->nodeTable->exist('e'.$fd)) {
            $this->nodeTable->del('e'.$fd);
        }
        // 用户 webSocket 客户端断开
        // todo swoole websocket 连接断开, 尚不支持 code reason, 返回 1006
        static::debug('[swoole]websocket close'.($error ? ' (open error)' : '').':'.$fd);
        $this->context->resolveWebSocketClose($fd, null, null, $error);
        return $this;
    }

















    public function start()
    {
        $this->swooleServer->start();
    }

    /**
     * 获取当前 Server 状态
     * @return array
     */
    public function status()
    {

    }

    /**
     * 获取当前服务器客户端连接数
     * @return int
     */
    public function clientCount()
    {
        return 10;
    }

    /**
     * 获取当前服务器客户端 连接列表
     * @param int $fd
     * @param int $row
     * @return array
     */
    public function clients($fd, $row = 100)
    {
        return [];
    }
}
