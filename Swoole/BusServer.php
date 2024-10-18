<?php
namespace Tanbolt\Websocket\Swoole;

use swoole_client;
use swoole_process;
use swoole_serialize;
use swoole_http_client;
use Tanbolt\Websocket\Bus;
use swoole_websocket_frame;
use swoole_websocket_server;
use Tanbolt\Websocket\Event;

class BusServer
{
    use Bus;

    // 当前节点发送消息给其他节点, 等待其他节点回复消息的超时时长 (毫秒)
    const WEB_SOCKET_MAX_WAIT = 3000;

    /**
     * @var swoole_websocket_server
     */
    protected $swooleServer;

    /**
     * 独立进程对象
     * @var swoole_process
     */
    protected $process;

    /**
     * 节点下线回调函数
     * @var callable
     */
    protected $offlineCallback;

    /**
     * 当期集群的 区域Url
     * @var string
     */
    protected $area = null;

    /**
     * 当前集群的 区域Id
     * @var int
     */
    protected $areaId = null;

    /**
     * 当期集群的 区域Hash
     * @var string
     */
    protected $areaHash = null;

    /**
     * 当前节点的 内网Url
     * @var string
     */
    protected $address = null;

    /**
     * 当前节点的 内网Id
     * @var int
     */
    protected $addressId = null;

    /**
     * 当前节点的 内网Hash
     * @var string
     */
    protected $addressHash = null;

    /**
     * 所有内网节点服务器缓存
     * [
     *      ip:port => [id => id, hash => hash]
     *      ......
     * ]
     * @var array
     */
    protected $nodes = [];

    /**
     * 以 id 作为 key 缓存内网节点信息, 方便快速获取(空间换时间)
     * [
     *     id => ip:port,
     *     ....
     * ]
     * @var array
     */
    protected $nodeIdLists = null;

    /**
     * 以 hash 作为 key 缓存内网节点信息
     * [
     *      hash=>id,
     *      ....
     * ]
     * @var array
     */
    protected $nodeHashLists = null;

    /**
     * 作为 webSocket 客户端, 已连接节点缓存
     * [
     *      ip:port => webSocket client object
     *      .....
     * ]
     * @var swoole_http_client[]
     */
    protected $connections = [];

    /**
     * 当前节点通过 webSocket 客户端发出请求 path 下 clientId 请求
     * 其他节点陆续返回各自节点 clientId, 在未完全接收到之前, 先把接收到的 clientId 缓存在该变量中
     * 待接收完毕, 将 client 设置到 $pathClients 变量中
     * [
     *    path => [reply_node_number, [clientId, .....]]
     * ]
     * @var array
     */
    protected $pathBuffers = [];

    /**
     * 除当前节点外, 其他所有节点指定 path 下的所有 clientId 缓存
     * 当 worker 需要获取指定 path 下所有 clientId 时, 其他节点不会每次都去请求
     * 而是返回 缓存 + 当前节点实时数据
     * [
     *     path => [updateTime, [clientId, .....]]
     *     .......
     * ]
     * @var array
     */
    protected $pathClients = [];

    /**
     * 同 $pathBuffers 其他节点陆续返回指定 group 下的 clientId 暂存变量
     * [
     *    group => [reply_node_number, [clientId, .....]]
     * ]
     * @var array
     */
    protected $groupBuffers = [];

    /**
     * 同 $pathClients 除当前节点外, 其他所有节点指定 group 下所有 clientId 缓存
     * [
     *     group => [updateTime, [clientId, .....]]
     *     .......
     * ]
     * @var array
     */
    protected $groupClients = [];

    /**
     * $pathClients,$groupClients 缓存 clientId 的过期时长
     * 节点内通信也有时间消耗, 设置同节点的 BusContext 和 BusServer 也有通信时间, 所以想完全实时是做不到的
     * @var int
     */
    protected $clientExpire = 10;

    /**
     * 从 worker 发来的消息缓存,当收到分片消息, 会缓存至一个完整消息触发回调
     * [
     *     workerId => [command, messageNo, len, data],
     *     ....
     * ]
     * @var array
     */
    protected $buffers = [];

    /**
     * 当前节点的 worker BusContext 发送了请求到 BusServer
     * 若请求数据不在当前节点, 就需要 BusServer 使用 webSocket 客户端向其他节点请求
     * 其他节点返回数据后, 将结果 sendMessage 给 worker BusContext
     * 这就出现了一个隐患, 如果其他节点迟迟不返回数据, worker BusContext 就会被阻塞, 无法继续工作
     * 所以做一个定时函数, BusServer 收到 worker BusContext 请求后, 立即设置一个定时器, n 毫秒后自动回复 null 给 worker BusContext
     * 如果在 n 毫秒内 busServer 收到了其他节点通过 webSocket 发来的数据, 取消定时器, 转发正确消息给 worker BusContext
     * @var array
     */
    protected $remoteTimers = [];

    /**
     * eventSource 连接进入后, 缓存其信息, 等待客户端发送 open 请求, open 后会从变量中删除, 用于验证 open 是否合法
     * [
     *    sessionId => [fd, workerId]
     * ]
     * @var array
     */
    protected $eventSources = [];

    /**
     * 客户端发送 open 请求后, 从 $eventSources 获取单向通道连接, 正式将客户端添加到 Bus, 并从 $eventSources 中移除
     * [
     *    fd => [sessionId, workerId]
     *    .....
     * ]
     * @var array
     */
    protected $eventClient = [];

    /**
     * 工具函数: 生成随机字符串
     * @return string
     */
    protected static function random()
    {
        return bin2hex(random_bytes(5));
    }

    /**
     * 工具函数:同步阻塞方式获取 一个 Http 请求结果
     * @param $host
     * @param $port
     * @param array $header
     * @return bool|mixed
     */
    protected static function getHttpResponse($host, $port, $header = [])
    {
        $cli = new swoole_client(SWOOLE_TCP);
        if (!$cli->connect($host, $port)) {
            $cli->close(true);
            return false;
        }
        $data = 'GET / HTTP/1.1'."\r\n".'Host: '.$host.':'.$port."\r\n";
        $header['x-server-client'] = 'client';
        foreach ($header as $key => $value) {
            $data .= $key.': '.$value."\r\n";
        }
        $data .= "\r\n";
        $cli->send($data);
        $receive = $cli->recv(65535);
        $cli->close();
        $pos = strpos($receive, "\r\n");
        if (!$pos) {
            return false;
        }
        $header = substr($receive, 0, $pos);
        $header = explode(' ', trim($header));
        if (count($header) < 3 || (int) $header[1] !== 200) {
            return false;
        }
        $pos = strpos($receive, "\r\n\r\n");
        if (!$pos) {
            return false;
        }
        return substr($receive, $pos + 4);
    }

    /**
     * BusServer constructor.
     * @param swoole_websocket_server $server
     * @param swoole_process $process
     * @param $node
     * @param $register
     */
    public function __construct(swoole_websocket_server $server, swoole_process $process, $node, $register)
    {
        ini_set("memory_limit", -1);

        // 只要一个 offline 回调, 其他的注销, 释放内存
        $this->offlineCallback = Event::$onNodeOffline;
        Event::destroy();

        // 初始化基本变量, 在内网集群中注册当前节点
        $this->swooleServer = $server;
        $this->process = $process;

        // todo 当前集群的区域ID
        $this->area = null;
        $this->areaId = null;
        $this->areaHash = null;

        // 当前节点内网ID
        $this->address = $node;
        $this->addNode($node)->registerCurrentNode($register);

        // 监听 worker 发来的消息
        swoole_event_add($this->process->pipe, function() {
            $this->onMessage($this->process->read());
        });
    }

    /**
     * todo 由 hash 获取区域ID
     * @param $hash
     * @return int
     */
    protected function getAreaId($hash)
    {
        return null;
    }

    /**
     * 当前集群 新增了一个节点
     * @param $address
     * @return $this
     */
    protected function addNode($address)
    {
        $this->nodeIdLists = $this->nodeHashLists = null;
        if (isset($this->nodes[$address])) {
            return $this;
        }
        $id = count($this->nodes) ? max(array_column($this->nodes, 'id')) + 1 : 0;
        $this->nodes[$address] = [
            'id' => $id,
            'hash' => self::random()
        ];
        return $this;
    }

    /**
     * 由于 ID 获取节点信息 (ip:port)
     * @param $id
     * @return bool|string
     */
    protected function getNodeAddress($id)
    {
        if ($this->nodeIdLists === null) {
            $this->nodeIdLists = [];
            foreach ($this->nodes as $address => $node) {
                $this->nodeIdLists[$node['id']] = $address;
            }
        }
        return $this->nodeIdLists[$id] ?? false;
    }

    /**
     * 由 Hash 值获取节点 ID
     * @param $hash
     * @return int
     */
    protected function getNodeAddressId($hash)
    {
        if ($this->nodeHashLists === null) {
            $this->nodeHashLists = [];
            foreach ($this->nodes as $address => $node) {
                $this->nodeHashLists[$node['hash']] = $node['id'];
            }
        }
        return $this->nodeHashLists[$hash] ?? null;
    }

    /**
     * 服务器启动 -> 向注册节点发送请求, 注册当前节点 -> 接收所有已注册节点
     * @param $register
     * @return $this
     */
    protected function registerCurrentNode($register)
    {
        $this->nodeIdLists = $this->nodeHashLists = null;
        $port = null;
        $register = trim($register);
        if (!empty($register)) {
            if (strpos($register, ':')) {
                list($register, $port) = explode(':', $register);
            } else {
                $port = 80;
            }
            $register = trim($register);
            $port = (int) $port;
            if ($register.':'.$port === $this->address) {
                $register = '';
            }
        }
        if (empty($register)) {
            $node = current($this->nodes);
            $this->addressId = (int) $node['id'];
            $this->addressHash = $node['hash'];
            return $this;
        }
        // 收到回复 (得到当前集群的所有节点内网地址)
        $data = static::getHttpResponse($register, $port, ['x-register' => $this->address]);
        if (!$data) {
            echo "Connect register $register failed\n";
            $this->swooleServer->shutdown();
            return $this;
        }
        Swoole::debug('[BusServer]get all node address');
        $this->nodes = json_decode($data, true);
        foreach ($this->nodes as $address => $node) {
            if ($address === $this->address) {
                $this->addressId = (int) $node['id'];
                $this->addressHash = $node['hash'];
                break;
            }
        }
        return $this->connectNodes();
    }

    /**
     * 作为 webSocket 客户端连接所有其他节点
     * @return $this
     */
    protected function connectNodes()
    {
        foreach ($this->nodes as $address => $node) {
            if ($address === $this->address) {
                continue;
            }
            $this->attachNode($address, $node);
        }
        return $this;
    }

    /**
     * 作为 webSocket 客户端 连接到指定的 其他节点
     * @param $address
     * @param $node
     * @return $this
     */
    protected function attachNode($address, $node)
    {
        if (isset($this->connections[$address])) {
            return $this;
        }
        Swoole::debug('[BusServer]connectTo:'.$address.'__'.$node['id'].'__'.$node['hash']);
        list($host, $port) = explode(':', $address);
        $cli = new swoole_http_client($host, $port);
        $cli->setHeaders([
           'x-server-client' => 'client'
        ]);
        $cli->on('message', function (swoole_http_client $cli, swoole_websocket_frame $frame) {
            $this->receiveMessageFromNode($cli, $frame);
        });
        $cli->on('error', function (swoole_http_client $cli) use ($address) {
            $this->disconnectNode($address);
        });
        $cli->on('close', function (swoole_http_client $cli) use ($address) {
            $this->disconnectNode($address);
        });
        $cli->upgrade('/', function (swoole_http_client $cli) {
            // 这里可以设置心跳 (不过集群是内网连接, 不用加心跳, 发送一个 open 字符串完事)
            $cli->push('open');
        });
        $this->connections[$address] = $cli;
        return $this;
    }

    /**
     * 由节点ID 获取 节点客户端对象
     * @param $nodeId
     * @return bool|swoole_http_client
     */
    protected function getNodeClient($nodeId)
    {
        $address = $this->getNodeAddress($nodeId);
        if (!$address || !isset($this->connections[$address])) {
            return false;
        }
        return $this->connections[$address];
    }

    /**
     * 作为 webSocket 客户端断开了与其他节点的连接
     * @param $address
     * @return $this
     */
    protected function disconnectNode($address)
    {
        Swoole::debug('[BusServer]node client disconnect:'.$address);
        Event::callListener($this->offlineCallback, [$address]);
        return $this;
    }

    /**
     * 发送消息给所有节点 (当前节点除外)
     * @param $command
     * @param Buffer $buffer
     * @param int $blocking
     * @return $this
     */
    protected function sendMessageToAllNode($command, Buffer $buffer, $blocking = 0)
    {
        $header = $this->makeSendMessageHeader(
            $buffer,
            $command,
            $blocking === true ? static::WEB_SOCKET_MAX_WAIT : (int) $blocking
        );
        foreach ($this->connections as $cli) {
            $this->sendBufferToNode($cli, $header, $buffer);
        }
        return $this;
    }

    /**
     * 发送消息给其他节点, 需要发消息给其他节点的情况有:
     * 1.获取其他节点指定 clientId 的客户端基本信息 (需回复结果)
     * 2.更新其他节点指定的 clientId 个人信息 (无需回复)
     * 3.断开其他节点指定的 clientId 客户端 (无需回复)
     *
     * 4.获取其他节点指定 path 下的所有 clientId (需回复结果)
     * 5.获取其他节点指定 group 下的所有 clientId (需回复结果)
     * 6.发送消息给其他节点符合条件的客户端 (无需回复)
     *
     * @param int $areaId 区域ID //todo 暂不支持跨地域部署
     * @param int $nodeId 节点ID
     * @param int $command 发送命令
     * @param Buffer $buffer 发送消息
     * @param int $blocking 阻塞时长
     *                      是:会等待对方节点回复消息后 reply worker, $blocking = true 阻塞时长使用 WEB_SOCKET_MAX_WAIT
     *                      否:仅发送消息给对方节点,应在调用该函数前自行reply worker，防止 worker 阻塞
     * @return $this
     */
    protected function sendMessageToNode($areaId, $nodeId, $command, Buffer $buffer, $blocking = 0)
    {
        $blocking = $blocking === true ? static::WEB_SOCKET_MAX_WAIT : (int) $blocking;
        $cli = $nodeId === null ? false : $this->getNodeClient($nodeId);
        if (!$cli) {
            // cli 不存在, 为保证 worker 不阻塞, 回复消息给 worker
            if ($blocking) {
                $buffer->reply(swoole_serialize::pack(null));
            }
            return $this;
        }
        return $this->sendBufferToNode($cli, $this->makeSendMessageHeader($buffer, $command, $blocking), $buffer);
    }

    /**
     * 生成发送到其他节点的消息头部
     * @param Buffer $buffer
     * @param $command
     * @param int $blocking
     * @return string
     */
    protected function makeSendMessageHeader(Buffer $buffer, $command, $blocking = 0)
    {
        // 需等待对方回复的, 做个定时器, 到时间后回复消息给 worker, 防止 worker 因该请求彻底阻塞
        if ($blocking) {
            $timerKey = $buffer->workerId.'_'.$buffer->messageNo;
            $this->remoteTimers[$timerKey] = swoole_timer_after($blocking, function () use ($timerKey, $buffer) {
                unset($this->remoteTimers[$timerKey]);
                $buffer->reply(swoole_serialize::pack(null));
            });
            $header = '1'.pack('nnn', $buffer->workerId, $buffer->messageNo, $command);
        } else {
            $header = '0'.pack('n', $command);
        }
        return $header;
    }

    /**
     * 正式发出消息到指定节点
     * @param swoole_http_client $cli
     * @param string $header
     * @param Buffer $buffer
     * @return $this
     */
    protected function sendBufferToNode(swoole_http_client $cli, $header, Buffer $buffer)
    {
        // 发送 text 消息给对方节点
        $cli->push($header.$buffer->forwardMessage);
        // 若还有二进制消息需转发, 继续发送
        if (count($buffer->forwardBinaries)) {
            foreach ($buffer->forwardBinaries as $binary) {
                $cli->push($binary, WEBSOCKET_OPCODE_BINARY);
            }
        }
        return $this;
    }

    /**
     * 监听从其他节点 通过 webSocket 回复的消息, 转发给 worker
     * 需要其他节点回复的消息一般包括: 指定用户的个人信息, 指定group下的所有clientId, 指定path下的所有clientId
     * @param swoole_http_client $cli
     * @param swoole_websocket_frame $frame
     * @return $this
     */
    protected function receiveMessageFromNode(swoole_http_client $cli, swoole_websocket_frame $frame)
    {
        $data = $frame->data;
        $header = unpack('nWorkerId/nMessageNo/nCommand', substr($data, 0, 6));
        if (count($header) !== 3) {
            return $this;
        }
        // 定时器还存在的情况才进行转发, 否则忽略; 不然会造成 worker 收到的消息顺序错乱
        $timerKey = $header['WorkerId'].'_'.$header['MessageNo'];
        if (!isset($this->remoteTimers[$timerKey])) {
            return $this;
        }
        $reply = null;
        $command = $header['Command'];
        $data = substr($data, 6);
        if ($command === Constants::CLIENT_GET) {
            Swoole::debug('[BusServer]get client info from other node');
            $reply = $data;
        } elseif ($command === Constants::PATH_GET) {
            Swoole::debug('[BusServer]get path clients from other node');
            $data = swoole_serialize::unpack($data);
            if (!is_array($data) || count($data) !== 2 || !is_string($data[0]) || !is_array($data[1])) {
                return $this;
            }
            $path = $data[0];
            if (!isset($this->pathBuffers[$path])) {
                return $this;
            }
            $this->pathBuffers[$path][0]++;
            $this->pathBuffers[$path][1] = array_merge($this->pathBuffers[$path][1], $data[1]);
            if ($this->pathBuffers[$path][0] >= count($this->connections)) {
                $reply = swoole_serialize::pack($this->pathBuffers[$path][1]);
                $this->pathClients[$path] = [time(), $this->pathBuffers[$path][1]];
                unset($this->pathBuffers[$path]);
            }
        } elseif ($command === Constants::GROUP_GET) {
            Swoole::debug('[BusServer]get group clients from other node');
            $data = swoole_serialize::unpack($data);
            if (!is_array($data) || count($data) !== 2 || !is_string($data[0]) || !is_array($data[1])) {
                return $this;
            }
            $group = $data[0];
            if (!isset($this->groupBuffers[$group])) {
                return $this;
            }
            $this->groupBuffers[$group][0]++;
            $this->groupBuffers[$group][1] = array_merge($this->groupBuffers[$group][1], $data[1]);
            if ($this->groupBuffers[$group][0] >= count($this->connections)) {
                $reply = swoole_serialize::pack($this->groupBuffers[$group][1]);
                $this->groupClients[$group] = [time(), $this->groupBuffers[$group][1]];
                unset($this->groupBuffers[$group]);
            }
        }
        if ($reply) {
            Swoole::debug('[BusServer]Forward other node message to BusContext');
            swoole_timer_clear($this->remoteTimers[$timerKey]);
            unset($this->remoteTimers[$timerKey]);
            $this->swooleServer->sendMessage('0'.pack('N', $header['MessageNo']).$reply, $header['WorkerId']);
        }
        return $this;
    }

    /**
     * 监听 worker 发来的消息, 必须回复消息
     * @param $message
     */
    protected function onMessage($message)
    {
        $buffer = null;
        $type = (int) substr($message, 0, 1);
        if ($type === 0) {
            // 完整消息
            $buffer = unpack('nWorkerId/nCommand/NMessageNo', substr($message, 1, 8));
            $buffer['Data'] = substr($message, 9);
            $buffer = new Buffer($buffer, $this->swooleServer);
        } elseif ($type === 1) {
            // 分片消息头
            $header = unpack('nWorkerId/nCommand/NMessageNo/NLen', substr($message, 1));
            $this->buffers[$header['WorkerId']] = [$header['Command'], $header['MessageNo'], $header['Len'], ''];
        } elseif ($type === 2) {
            // 分片消息 片段
            $workerId = unpack('n', substr($message, 1, 2));
            $workerId = $workerId[1];
            if (isset($this->buffers[$workerId])) {
                $this->buffers[$workerId][3] .= substr($message, 3);
                if (strlen($this->buffers[$workerId][3]) >= $this->buffers[$workerId][2]) {
                    $buffer = new Buffer([
                        'WorkerId' => $workerId,
                        'Command' => $this->buffers[$workerId][0],
                        'MessageNo' => $this->buffers[$workerId][1],
                        'Data' => $this->buffers[$workerId][3]
                    ], $this->swooleServer);
                    unset($this->buffers[$workerId]);
                }
            }
        }
        if ($buffer) {
            $this->resolveMessage($buffer);
        }
    }

    /**
     * 主动发送消息给 worker (通常是处理 polling 相关操作使用)
     * @param $workerId
     * @param $command
     * @param $data
     * @return $this
     */
    protected function sendMessageToWorker($workerId, $command, $data)
    {
        $this->swooleServer->sendMessage('1'.pack('n', $command).$data, $workerId);
        return $this;
    }

    /**
     * 处理从 worker 发送的消息, 执行函数中使用 $buffer->reply 回复消息
     * @param Buffer $buffer
     */
    protected function resolveMessage(Buffer $buffer)
    {
        switch ($buffer->command) {
            case Constants::NODE_REGISTER:
                $this->nodeRegisterCommand($buffer);
                break;
            case Constants::NODE_ADD:
                $this->nodeAddCommand($buffer);
                break;
            case Constants::NODE_ID:
                $this->nodeIdCommand($buffer);
                break;
            case Constants::CLIENT_EVENT_CONNECT:
                $this->clientEventConnectCommand($buffer);
                break;
            case Constants::CLIENT_EVENT_EMIT:
                $this->clientEventEmitCommand($buffer);
                break;
            case Constants::CLIENT_EVENT_CLOSE:
                $this->clientEventCloseCommand($buffer);
                break;
            case Constants::CLIENT_REQUEST_OPEN:
                $this->clientRequestOpenCommand($buffer);
                break;
            case Constants::CLIENT_REQUEST_EMIT:
                $this->clientRequestEmitCommand($buffer);
                break;
            case Constants::CLIENT_REQUEST_CLOSE:
                $this->clientRequestCloseCommand($buffer);
                break;
            case Constants::CLIENT_ADD:
                $this->clientAddCommand($buffer);
                break;
            case Constants::CLIENT_UPHEADER:
                $this->clientUpdateHeaderCommand($buffer);
                break;
            case Constants::CLIENT_UPDATE:
                $this->clientUpdateCommand($buffer);
                break;
            case Constants::CLIENT_GET:
                $this->clientGetCommand($buffer);
                break;
            case Constants::CLIENT_DROP:
                $this->clientDropCommand($buffer);
                break;
            case Constants::CLIENT_CLOSE:
                $this->clientCloseCommand($buffer);
                break;
            case Constants::PATH_GET:
                $this->pathGetCommand($buffer);
                break;
            case Constants::GROUP_GET:
                $this->groupGetCommand($buffer);
                break;
            case Constants::FORWARD:
                $this->forwardCommand($buffer);
                break;
            case Constants::EMIT_CLIENTS:
                $this->emitClientsCommand($buffer);
                break;
            default:
                $buffer->reply(' ');
                break;
        }
    }

    /**
     * workerMessage: 当前节点作为注册 -> 收到了一个新节点注册请求 -> 返回已注册节点数组
     * @param Buffer $buffer
     * @return $this
     */
    protected function nodeRegisterCommand(Buffer $buffer)
    {
        // 新增节点
        $this->addNode($buffer->message);
        // 返回消息: 所有已注册节点
        $buffer->reply(swoole_serialize::pack($this->nodes));
        swoole_timer_after(500, function() use ($buffer) {
            // 通知其他已上线的节点， 有新节点加入了
            foreach ($this->nodes as $address => $node) {
                if ($address === $this->address || $address === $buffer->message) {
                    continue;
                }
                $this->notifyNodeChange($address);
            }
            // 作为 webSocket 客户端连接新上线节点
            $this->connectNodes();
        });
        return $this;
    }

    /**
     * 发送 http 请求给指定 $node, 告知地址为 $address 的新节点上线, 快欢迎
     * @param $address
     * @param int $tryTimes
     * @return $this|BusServer
     */
    protected function notifyNodeChange($address, $tryTimes = 0)
    {
        list($host, $port) = explode(':', $address);
        $data = $this->getHttpResponse($host, $port, ['x-nodes' => json_encode($this->nodes)]);
        if ($data === 'ok' || $tryTimes > 3) {
            return $this;
        }
        usleep(10);
        return $this->notifyNodeChange($address, ++$tryTimes);
    }

    /**
     * workerMessage: 作为普通节点 -> 收到了节点变化通知
     * @param Buffer $buffer
     * @return $this'
     */
    protected function nodeAddCommand(Buffer $buffer)
    {
        $buffer->reply('ok');
        $this->nodeIdLists = $this->nodeHashLists = null;
        $this->nodes = swoole_serialize::unpack($buffer->message);;
        // 作为 webSocket 客户端连接新上线节点
        $this->connectNodes();
        return $this;
    }

    /**
     * workerMessage: 当前节点的 群与ID 和 内网节点ID
     * @param Buffer $buffer
     * @return $this
     */
    protected function nodeIdCommand(Buffer $buffer)
    {
        $buffer->reply(swoole_serialize::pack([
            'areaId' => $this->areaId,
            'areaHash' => $this->areaHash,
            'nodeId' => $this->addressId,
            'nodeHash' => $this->addressHash,
        ]));
        return $this;
    }

    /**
     * workerMessage: eventSource 连接进入, 生成 randId 等待客户端的 post request open, 失败返回 fail
     * @param Buffer $buffer
     * @return $this
     */
    protected function clientEventConnectCommand(Buffer $buffer)
    {
        $fd = (int) $buffer->message;
        Swoole::debug('[BusServer]client EventSource connect:'.$fd);
        $sessionId = self::random();
        $this->eventSources[$sessionId] = [$fd, $buffer->workerId];
        $buffer->reply($sessionId);
        return $this;
    }

    /**
     * workerMessage: 服务器端要主动发消息给 eventSource
     * @param Buffer $buffer
     * @return $this
     */
    protected function clientEventEmitCommand(Buffer $buffer)
    {
        $buffer->reply('ok');
        $data = swoole_serialize::unpack($buffer->message);
        $sendQueue = [];
        foreach ($data['clients'] as $sessionId) {
            if (($user = $this->memGetClient($sessionId)) && is_array($user[5])) {
                list($fd, $workerId) = $user[5];
                if (!isset($sendQueue[$workerId])) {
                    $sendQueue[$workerId] = [];
                }
                $sendQueue[$workerId][] = $sessionId;
            }
        }
        foreach ($sendQueue as $workerId => $clients) {
            $data['clients'] = $clients;
            Swoole::debug('[BusServer]emit message to eventSource clients');
            $this->sendMessageToWorker($workerId, Constants::CLIENT_EVENT_EMIT, swoole_serialize::pack($data));
        }
        return $this;
    }

    /**
     * workerMessage: eventSource 断开后回调, 若是已open的客户端, 返回 sessionId(fd 的 hash值), 否则返回 fail
     * @param Buffer $buffer
     * @return $this
     */
    protected function clientEventCloseCommand(Buffer $buffer)
    {
        $fd = (int) $buffer->message;
        Swoole::debug('[BusServer]client EventSource disconnect:'.$fd);
        $sessionId = null;
        $workerId = null;
        $opened = false;
        foreach ($this->eventSources as $random => $item) {
            if ($item[0] === $fd) {
                Swoole::debug('[BusServer]client remove from eventSources:'.$fd);
                $sessionId = $random;
                $workerId = $item[1];
                unset($this->eventSources[$random]);
                break;
            }
        }
        if (isset($this->eventClient[$fd])) {
            Swoole::debug('[BusServer]client remove from eventClient:'.$fd);
            list($sessionId, $workerId) = $this->eventClient[$fd];
            if ($this->memExistClient($sessionId)) {
                $opened = true;
            }
            unset($this->eventClient[$fd]);
        }
        $clientInfo = swoole_serialize::pack(compact('fd', 'sessionId', 'workerId', 'opened'));
        if ($buffer->workerId === $workerId) {
            $buffer->reply($clientInfo);
        } else {
            // 直接个当前发消息的 worker 回复, 并通知维护 eventSource 的 worker
            $buffer->reply(swoole_serialize::pack(null));
            $this->sendMessageToWorker($workerId, Constants::CLIENT_EVENT_CLOSE, $clientInfo);
        }
        return $this;
    }

    /**
     * workerMessage: 客户端 post request open 操作
     * 一定是通知其他节点的, 如果是当前节点的, 在 BusContext 中就直接执行 Client_Add 命令了
     * @param Buffer $buffer
     * @return $this
     */
    protected function clientRequestOpenCommand(Buffer $buffer)
    {
        $buffer->reply('ok');
        $info = swoole_serialize::unpack($buffer->message);
        $client = $info['client'];
        $areaId = $this->getAreaId($client['area']);
        $nodeId = $this->getNodeAddressId($client['node']);
        Swoole::debug('[BusServer]polling request open to other node:'.$client['session']);
        $buffer->forwardMessage = swoole_serialize::pack(['sessionId' => $client['session'], 'user' => $info['user']]);
        $this->sendMessageToNode($areaId, $nodeId,Constants::CLIENT_REQUEST_OPEN, $buffer);
        return $this;
    }

    /**
     * workerMessage: post request emit 操作
     * @param Buffer $buffer
     * @return $this
     */
    protected function clientRequestEmitCommand(Buffer $buffer)
    {
        $buffer->reply('ok');
        $info = swoole_serialize::unpack($buffer->message);
        $client = $info['client'];
        $areaId = $this->getAreaId($client['area']);
        $nodeId = $this->getNodeAddressId($client['node']);
        if ($areaId === $this->areaId && $nodeId === $this->addressId) {
            // 接受消息的 eventSource 客户端就在当前节点
            if (($user = $this->memGetClient($client['session'])) && is_array($user[5])) {
                list($fd, $workerId) = $user[5];
                $this->sendMessageToWorker($workerId, Constants::CLIENT_REQUEST_EMIT, $buffer->message);
            }
            Swoole::debug('[BusServer]polling request emit in this node('.($user ? 'yes' : 'no').'):'.$client['session']);
        } else {
            // 通知其他节点收到 request 消息
            Swoole::debug('[BusServer]polling request emit to other node:'.$client['session']);
            $buffer->forwardMessage = $buffer->message;
            $this->sendMessageToNode($areaId, $nodeId,Constants::CLIENT_REQUEST_EMIT, $buffer);
        }
        return $this;
    }

    /**
     * workerMessage: post request close 操作
     * @param Buffer $buffer
     * @return $this
     */
    protected function clientRequestCloseCommand(Buffer $buffer)
    {
        $buffer->reply('ok');
        $info = swoole_serialize::unpack($buffer->message);
        $client = $info['client'];
        $areaId = $this->getAreaId($client['area']);
        $nodeId = $this->getNodeAddressId($client['node']);
        if ($areaId === $this->areaId && $nodeId === $this->addressId) {
            // 要关闭的 eventSource 客户端就在当前节点
            if (($user = $this->memGetClient($client['session'])) && is_array($user[5])) {
                list($fd, $workerId) = $user[5];
                $closeFrame = $info['close'] ?? [];
                if (!isset($closeFrame['code'])) {
                    $closeFrame['code'] = null;
                }
                if (!isset($closeFrame['reason'])) {
                    $closeFrame['reason'] = '';
                }
                $closeFrame['session'] = $client['session'];
                $closeFrame['polling'] = true;
                $this->sendMessageToWorker($workerId, Constants::CLIENT_REQUEST_CLOSE, swoole_serialize::pack($closeFrame));
            }
            Swoole::debug('[BusServer]polling request close in this node('.($user ? 'yes' : 'no').'):'.$client['session']);
        } else {
            // 通知其他节点关闭 eventSource
            Swoole::debug('[BusServer]polling request close to other node:'.$client['session']);
            $buffer->forwardMessage = $buffer->message;
            $this->sendMessageToNode($areaId, $nodeId,Constants::CLIENT_REQUEST_CLOSE, $buffer);
        }
        return $this;
    }

    /**
     * workerMessage: 新增用户, sessionId 是在当前节点唯一
     * yes:  添加成功
     * no:   添加失败
     * none: 要添加的 polling eventSource 不在请求的worker上
     * @param Buffer $buffer
     * @return $this
     */
    protected function clientAddCommand(Buffer $buffer)
    {
        $request = swoole_serialize::unpack($buffer->message);
        if (!is_array($request) || !isset($request['sessionId']) || !isset($request['polling'])) {
            Swoole::debug('[BusServer]add user client request error');
            $buffer->reply('no');
            return $this;
        }
        $sessionId = $request['sessionId'];
        $polling = (bool) $request['polling'];
        unset($request['sessionId'], $request['polling']);
        // webSocket 客户端
        if (!$polling) {
            $request['polling'] = null;
            $this->memAddClient($sessionId, $request);
            Swoole::debug('[BusServer]add webSocket user client:'.$sessionId);
            $buffer->reply('yes');
            return $this;
        }
        // polling 客户端
        $fd = $workerId = null;
        if (isset($this->eventSources[$sessionId])) {
            list($fd, $workerId) = $this->eventSources[$sessionId];
            unset($this->eventSources[$sessionId]);
        }
        if ($fd === null || $workerId === null) {
            $buffer->reply('none');
            return $this;
        }
        $result = 'yes';
        $this->eventClient[$fd] = [$sessionId, $workerId];
        $request['polling'] = [$fd, $workerId];
        $this->memAddClient($sessionId, $request);
        if ($workerId === $buffer->workerId) {
            $buffer->reply($result);
        } else {
            $buffer->reply('none');
            $this->sendMessageToWorker($workerId, Constants::CLIENT_REQUEST_OPEN, swoole_serialize::pack([
                'sessionId' => $sessionId,
                'result' => $result
            ]));
        }
        return $this;
    }

    /**
     * 更新自定义 Header
     * @param Buffer $buffer
     * @return $this
     */
    protected function clientUpdateHeaderCommand(Buffer $buffer)
    {
        $client = swoole_serialize::unpack($buffer->message);
        if (isset($client['sessionId']) && isset($client['headers'])) {
            Swoole::debug('[BusServer]update user custom header:'.$client['sessionId']);
            $this->memUpdateHeader($client['sessionId'], $client['headers']);
        }
        $buffer->reply('ok');
        return $this;
    }

    /**
     * workerMessage: 获取用户信息 $buffer->message = clientId[__]
     * @param Buffer $buffer
     * @return $this
     */
    protected function clientGetCommand(Buffer $buffer)
    {
        $remote = substr($buffer->message, -2) === '__';
        $clientId = $remote ? substr($buffer->message, 0, -2) : $buffer->message;
        $client = Swoole::unpackClientId($clientId);
        if (!$client) {
            $buffer->reply(swoole_serialize::pack(null));
            return $this;
        }
        if ($client['polling']) {
            $areaId = $this->getAreaId($client['area']);
            $nodeId = $this->getNodeAddressId($client['node']);
        } else {
            $areaId = strlen($client['area']) ? (int) $client['area'] : null;
            $nodeId = (int) $client['node'];
        }
        $sessionId = $client['session'];
        // 要获取的用户刚好在当前节点, 直接返回给 worker 进程
        if ($areaId === $this->areaId && $nodeId === $this->addressId) {
            $info = $this->memGetClient($sessionId);
            if (is_array($info)) {
                array_unshift($info, $areaId, $nodeId, $sessionId);
            } else {
                $info = null;
            }
            Swoole::debug('[BusServer]get client info in this node:'.$clientId.'['.($info ? 'yes' : 'no').']');
            $buffer->reply(swoole_serialize::pack($info));
            return $this;
        }
        // 不在当前节点, 且是其他节点来取信息的, 返回 null 给 worker
        if ($remote) {
            Swoole::debug('[BusServer]reply get client info failed to other node:'.$clientId);
            $buffer->reply(swoole_serialize::pack(null));
            return $this;
        }
        Swoole::debug('[BusServer]get client info (send message to other node) :'.$clientId);
        // 不在当前节点, 但在其他节点, 请求对应节点, 对应节点应返回信息给当前 BusServer, BusServer 转发给 worker
        $buffer->forwardMessage = $buffer->message.'__';
        $this->sendMessageToNode($areaId, $nodeId,Constants::CLIENT_GET, $buffer, 2000);
        return $this;
    }

    /**
     * workerMessage: 更新用户信息, 消息解包后为: [clientId, group, attr, [remote]] $areaId, $nodeId, $sessionId
     * @param Buffer $buffer
     * @return $this
     */
    protected function clientUpdateCommand(Buffer $buffer)
    {
        // 无需等待结果, 先回复消息给 worker
        $buffer->reply(swoole_serialize::pack(null));
        $updates = swoole_serialize::unpack($buffer->message);
        $client = null;
        if (count($updates) < 4) {
            return $this;
        }
        list($clientId, $addGroup, $subGroup, $alterAttr) = $updates;
        $remote = isset($updates[4]) ? (bool) $updates[4] : false;
        $client = Swoole::unpackClientId($clientId);
        if (!$client) {
            return $this;
        }
        if ($client['polling']) {
            $areaId = $this->getAreaId($client['area']);
            $nodeId = $this->getNodeAddressId($client['node']);
        } else {
            $areaId = strlen($client['area']) ? (int) $client['area'] : null;
            $nodeId = (int) $client['node'];
        }
        $sessionId = $client['session'];
        // 要更新的用户刚好在当前节点, 直接更新
        if ($areaId === $this->areaId && $nodeId === $this->addressId) {
            Swoole::debug('[BusServer]update client info in this node:'.$clientId);
            if (count($addGroup)) {
                $this->memAddClientGroup($sessionId, $addGroup);
            }
            if (count($subGroup)) {
                $this->memDelClientGroup($sessionId, $subGroup);
            }
            if (count($alterAttr)) {
                $this->memSetClientAttr($sessionId, $alterAttr);
            }
            return $this;
        }
        // 从其他节点来设置当前节点用户的, 但又不存在, 因为对方节点并不需要回复, 忽略之
        if ($remote) {
            Swoole::debug('[BusServer]update client info failed (request from other node) :'.$clientId);
            return $this;
        }
        // 不在当前节点, 请求其他节点更新用户信息
        Swoole::debug('[BusServer]update client info (send message to other node) :'.$clientId);
        $buffer->forwardMessage = swoole_serialize::pack([$clientId, $addGroup, $subGroup, $alterAttr, 1]);
        $this->sendMessageToNode($areaId, $nodeId,Constants::CLIENT_UPDATE, $buffer);
        return $this;
    }

    /**
     * workerMessage: worker 进程 BusContext 断开连接后, 发消息过来请求清除用户信息, 肯定是当前节点的用户
     * @param Buffer $buffer
     * @return $this
     */
    protected function clientDropCommand(Buffer $buffer)
    {
        $buffer->reply('ok');
        Swoole::debug('[BusServer]drop client:'.$buffer->message);
        list($sessionId, $isPolling) = explode('_', $buffer->message);
        $this->memDropClient($sessionId);
        return $this;
    }

    /**
     * 断开用户连接, 肯定是断开其他节点上的用户 (若断开本节点的用户, 在 worker 进程就直接处理了)
     * @param Buffer $buffer
     * @return $this
     */
    protected function clientCloseCommand(Buffer $buffer)
    {
        // 无需等待结果, 先回复消息给 worker
        $buffer->reply(swoole_serialize::pack(null));
        // [polling=>bool, area=>int|string, node=>int|string, session=>string|int, code=>int, reason=>string]
        $client = swoole_serialize::unpack($buffer->message);
        if ($client['polling']) {
            $areaId = $this->getAreaId($client['area']);
            $nodeId = $this->getNodeAddressId($client['node']);
        } else {
            $areaId = strlen($client['area']) ? (int) $client['area'] : null;
            $nodeId = (int) $client['node'];
        }
        $buffer->forwardMessage = $buffer->message;
        $this->sendMessageToNode($areaId, $nodeId,Constants::CLIENT_CLOSE, $buffer);
        return $this;
    }

    /**
     * workerMessage: 获取指定 path 下的 clientId
     * @param Buffer $buffer
     * @return $this
     */
    protected function pathGetCommand(Buffer $buffer)
    {
        $path = swoole_serialize::unpack($buffer->message);
        // 参数错误
        if (!is_array($path) || !($count = count($path))) {
            $buffer->reply(swoole_serialize::pack(null));
            return $this;
        }
        $path = $path[0];
        $remote = $count > 1;
        // 未集群部署, 且就当前一个节点, 返回当前节点符合条件的即可
        if (!$remote && !count($this->connections)) {
            Swoole::debug('[BusServer]get path clients, only one node');
            $buffer->reply(swoole_serialize::pack(
                $this->packClientIds($this->memGetPathClient($path))
            ));
            return $this;
        }
        // 集群部署, 但已经获取的其他节点 clientIds 缓存未过期, 直接返回
        $hash = md5(serialize($path));
        if (!$remote && isset($this->pathClients[$hash]) && $this->pathClients[$hash][0] > time() - $this->clientExpire) {
            Swoole::debug('[BusServer]get path clients from cache');
            $buffer->reply(swoole_serialize::pack($this->pathClients[$hash][1]));
            return $this;
        }
        // 当前节点 group 下的 clientIds
        $clientIds = $this->packClientIds($this->memGetPathClient($path));
        // 别的节点来取数据, 仅返回当前节点
        if ($remote) {
            Swoole::debug('[BusServer]get path clients, reply to other node');
            $buffer->reply(swoole_serialize::pack([$hash, $clientIds]));
            return $this;
        }
        // 先记录下当前节点符合条件的, 再向其他所有节点发送请求, 要求返回 clientIds
        Swoole::debug('[BusServer]get path clients, wait other node reply');
        $this->pathBuffers[$hash] = [0, $clientIds];
        $buffer->forwardMessage = swoole_serialize::pack([$path, 1]);
        $this->sendMessageToAllNode(Constants::PATH_GET, $buffer, 5000);
        return $this;
    }

    /**
     * workerMessage: 获取指定 group 下的 clientId
     * @param Buffer $buffer
     * @return $this
     */
    protected function groupGetCommand(Buffer $buffer)
    {
        $group = swoole_serialize::unpack($buffer->message);
        // 参数错误
        if (!is_array($group) || !($count = count($group))) {
            $buffer->reply(swoole_serialize::pack(null));
            return $this;
        }
        $group = $group[0];
        $remote = $count > 1;
        // 未集群部署, 且就当前一个节点, 返回当前节点符合条件的即可
        if (!$remote && !count($this->connections)) {
            Swoole::debug('[BusServer]get group clients, only one node');
            $buffer->reply(swoole_serialize::pack(
                $this->packClientIds($this->memGetGroupClient($group))
            ));
            return $this;
        }
        // 集群部署, 但已经获取的其他节点 clientIds 缓存未过期, 直接返回
        $hash = md5(serialize($group));
        if (!$remote && isset($this->groupClients[$hash]) && $this->groupClients[$hash][0] > time() - $this->clientExpire) {
            Swoole::debug('[BusServer]get group clients from cache');
            $buffer->reply(swoole_serialize::pack($this->groupClients[$hash][1]));
            return $this;
        }
        // 当前节点 group 下的 clientIds
        $clientIds = $this->packClientIds($this->memGetGroupClient($group));
        // 别的节点来取数据, 仅返回当前节点
        if ($remote) {
            Swoole::debug('[BusServer]get group clients, reply to other node');
            $buffer->reply(swoole_serialize::pack([$hash, $clientIds]));
            return $this;
        }
        // 先记录下当前节点符合条件的, 再向其他所有节点发送请求, 要求返回 clientIds
        Swoole::debug('[BusServer]get group clients, wait other node reply');
        $this->groupBuffers[$hash] = [0, $clientIds];
        $buffer->forwardMessage = swoole_serialize::pack([$group, 1]);
        $this->sendMessageToAllNode(Constants::GROUP_GET, $buffer, 5000);
        return $this;
    }

    /**
     * 打包当前节点下的 sessionId => clientId
     * @param array $sessionIds
     * @return array
     */
    protected function packClientIds(array $sessionIds = [])
    {
        return array_map(function ($sessionId) {
            if (is_numeric($sessionId)) {
                return Swoole::packClientId($this->areaId, $this->addressId, $sessionId);
            }
            return Swoole::packClientId($this->areaHash, $this->addressHash, $sessionId, true);
        }, $sessionIds);
    }

    /**
     * 群发消息给其他节点
     * @param Buffer $buffer
     * @return $this
     */
    protected function forwardCommand(Buffer $buffer)
    {
        // worker 无需等待, 非阻塞异步发送. 立即回复消息给 worker
        $buffer->reply(swoole_serialize::pack(null));
        // worker 发来的消息是 pack 好的, 收到消息后 unpack, 以便使用 webSocket 客户端进行转发
        if (!$buffer->unpack()) {
            return $this;
        }
        // 群发
        if ($buffer->emitType !== Constants::FORWARD_CLIENT) {
            Swoole::debug('[BusServer]forward message to other nodes');
            $this->sendMessageToAllNode(Constants::FORWARD, $buffer);
            return $this;
        }
        // 指定了具体发送对象, 整理要接收转发消息的节点, 并重新打包 scope
        $clients = [];
        foreach ($buffer->emitScope as $client) {
            if ($client['polling']) {
                $areaId = $this->getAreaId($client['area']);
                $nodeId = $this->getNodeAddressId($client['node']);
            } else {
                $areaId = strlen($client['area']) ? (int) $client['area'] : null;
                $nodeId = (int) $client['node'];
            }
            // 待接收转发消息的节点 竟然是当前节点, 显然不正确
            if ($areaId === $this->areaId && $nodeId === $this->addressId) {
                continue;
            }
            $hashId = $areaId !== null ? $areaId.'_'.$nodeId : $nodeId;
            if (!isset($clients[$hashId])) {
                $clients[$hashId] = [$areaId, $nodeId, []];
            }
            $clients[$hashId][2][$client['session']] = $client['polling'];
        }
        swoole::debug('[BusServer]forward message to other nodes');
        foreach ($clients as $client) {
            $buffer->repack($client[2]);
            $this->sendMessageToNode($client[0], $client[1],Constants::FORWARD, $buffer);
        }
        return $this;
    }

    /**
     * 获取指定条件的客户端 sessionId, 返回
     * [
     *      sessionId => isPolling,
     *      .....
     * ]
     * @param Buffer $buffer
     * @return $this
     */
    protected function emitClientsCommand(Buffer $buffer)
    {
        $scope = swoole_serialize::unpack($buffer->message);
        $path = $group = null;
        switch ($scope['scopeType']) {
            case Constants::FORWARD_PATH:
                $path = $scope['scope'];
                break;
            case Constants::FORWARD_GROUP:
                $group = $scope['scope'];
                break;
            case Constants::FORWARD_WORLD:
                break;
            default:
                $buffer->reply(swoole_serialize::pack(null));
                return $this;
        }
        // 同时限定了 path 和 group
        $clients = [];
        if ($path) {
            $clients = $this->memGetPathClient($path, true);
        } elseif ($group) {
            $clients = $this->memGetGroupClient($group, true);
        } else {
            foreach ($this->clients as $sessionId => $client) {
                $clients[$sessionId] = (bool) $client[5];
            }
        }
        $exSession = $scope['exSession'];
        if ($clients && $exSession && isset($clients[$exSession])) {
            unset($clients[$exSession]);
        }
        $buffer->reply(swoole_serialize::pack($clients));
        return $this;
    }
}
