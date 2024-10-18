<?php
namespace Tanbolt\Websocket\Swoole;

use swoole_process;
use swoole_serialize;
use swoole_http_request;
use swoole_http_response;
use swoole_websocket_frame;
use swoole_websocket_server;
use Tanbolt\Websocket\Event;
use Swoole\Coroutine\Channel;
use Tanbolt\Websocket\Socket;
use Tanbolt\Websocket\Binary;
use Tanbolt\Websocket\Binaries;
use Tanbolt\Websocket\ContextInterface;

class BusContext implements ContextInterface
{
    // 系统内部的一些关闭帧
    const ADD = 3001;
    const ADD_REASON = 'add client failed';

    const TIMEOUT = 3002;
    const TIMEOUT_REASON = 'wait custom headers timeout';

    const CREATE = 3003;
    const CREATE_REASON = 'Create socket failed';

    const GET = 3004;
    const GET_REASON = 'Get client info failed';

    const OPEN = 3005;

    /**
     * @var Swoole
     */
    protected $swoole;

    /**
     * @var swoole_websocket_server
     */
    protected $swooleServer;

    /**
     * 当前 workerId
     * @var int
     */
    protected $workerId;

    /**
     * 用于存储当前节点用户信息的独立进程
     * @var swoole_process
     */
    protected $process;

    /**
     * ping 间隔时间
     * @var int
     */
    protected $pingInterval;

    /**
     * swoole 协程通道对象
     * -> Socket 对象发出请求, 请求转交给 BusContext 处理
     * -> BusContext 将请求转换为消息，发送给 BusServer(swooleProcess), loop 等待 channel pop 消息
     * -> BusServer(swooleProcess) 收到消息后异步处理, 待处理完毕通过 sendMessage 给发送消息的 worker
     * -> Worker 绑定 onPipeMessage, 收到 BusServer(swooleProcess) 返回的消息, channel->push(message)
     * -> BusContext pop 得到消息, 进行下一步操作
     *
     * 设计初衷, 为什么加了一个 channel
     * 1. 本来 BusContext->BusServer, 消息发出后, 等待 BusServer->BusContext 返回消息即可.
     *    确实可以这样做, 但这样会带来一个问题, 二者通信是先进先出.
     *    worker1[BusContext]  worker2[BusContext] 先后发消息给 BusServer, BusServer 也必须按照同样顺序返回消息
     *    这样就需要 BusServer 控制消息顺序, 另外 BusContext 可能是分片发送消息的, 还需要 BusServer 处理分片
     *    而 BusServer 仅做为一个独立进程, 本来就要处理所有 worker 发送的请求, 若出现问题会影响所有 worker, 放弃这种方案
     * 2. BusContext 发消息给 BusServer, 消息中告知自己的 workerId 和 messageNo, 发出消息后 worker 转到协程, 让出资源
     *    BusServer 处理完之后异步 pipMessage 给 worker, 返回的消息中需包含 messageNo,
     *    因为不同消息处理时长可能不同, channel 也是先进先出的管道, 这可能造成后发出的消息先收到了回复, 造成消息错乱
     *    带上 messageNo, BusContext 会根据消息序号决定是直接处理回复, 还是等一下之前发出消息的回复.
     *    确定已缓存回复是按照顺序的了, 依次 push 到 channel,  channel 依次 pop, 继续操作
     * @var Channel
     */
    protected $channel;

    /**
     * 当前集群的区域ID
     * @var int
     */
    protected $areaId;

    /**
     * 当前集群的区域Hash
     * @var string
     */
    protected $areaHash;

    /**
     * 当前节点内网ID
     * @var int
     */
    protected $nodeId;

    /**
     * 当前节点的内网Hash
     * @var string
     */
    protected $nodeHash;

    /**
     * 当前节点内网地址
     * @var string
     */
    protected $nodeAddress;

    /**
     * 与 Process 通信的消息头缓存, 避免每次都 pack, 空间换时间
     * @var array
     */
    protected $messagePacks;

    /**
     * 当前向 Process 已发送消息的 messageNo
     * @var int
     */
    protected $messageNo;

    /**
     * 当前已接收的来自 process 的 messages
     * @var array
     */
    protected $messageReceive;

    /**
     * 收到其他节点发来的消息, 默认回复值
     * @var string
     */
    protected $socketDefaultMessage;

    /**
     * worker 向 process 每次发送消息字节数 (8192 || 2048)
     * https://wiki.swoole.com/wiki/page/216.html
     * @var int
     */
    protected $chunkSize;

    /**
     * 从其他节点发来的消息, 要群发到当前节点的客户端, 该变量用来缓存群发类型和群发范围
     * @var array
     */
    private $messageEmit = [];

    /**
     * 等待已打开的 webSocket 发送自定义 Header, 若超时, 关闭连接
     * 该变量 存储 定时器
     * @var array
     */
    private $waitHeaders = [];

    /**
     * 客户端发送来的 webSocket 普通 text 消息, 若 text 消息依赖后续二进制消息, 先缓存到该变量
     * 待收到所依赖的二进制消息后, 在与 text 消息合并触发回调
     * [
     *    fd => [count, code, data] //[需要二进制消息个数, 消息头, 消息实体]
     *    ....
     * ]
     * @var array
     */
    private $messages = [];

    /**
     * webSocket 二进制消息缓存, 当缓存个数达到 $messages 所需, 弹出缓存, 与 $message 组合, 触发回调, 注销 $message 对应缓存
     * [
     *    fd => [binary,binary...]
     *    .....
     * ]
     * @var array
     */
    private $messageBuffer = [];

    /**
     * 服务端推送消息给普通客户端, 等待回复后进行回调的函数缓存
     * @var array
     */
    protected $replyListeners = [];

    /**
     * 维护 polling 方式的 eventSource response 对象
     * @var array
     */
    protected $pollingResponses = [];

    /**
     * BusContext constructor.
     * @param Swoole $swoole
     * @param swoole_process $process
     * @param $address
     */
    public function __construct(Swoole $swoole, swoole_process $process, $address)
    {
        $this->swoole = $swoole;
        $this->swooleServer = $swoole->swooleServer();
        $this->process = $process;
        $this->pingInterval = (int) $swoole->getOptions('ping_interval');
        $this->channel = new Channel();
        $this->areaId = $this->nodeId = null;
        $this->nodeAddress = $address;
        $this->workerId = $this->swooleServer->worker_id;
        $this->chunkSize = PHP_OS === 'Linux' ? 8192 : 2048;
        $this->messagePacks = [
            'workerId' => pack('n', $this->workerId)
        ];
        $this->messageNo = 0;
        $this->messageReceive = [];
        $this->socketDefaultMessage = swoole_serialize::pack(null);
    }

    /**
     * 设置当前的 区域信息/节点信息
     * @return $this
     */
    protected function initClientInfo()
    {
        if ($this->nodeId === null) {
            $node = $this->cmdBusServer(Constants::NODE_ID, 'id');
            $node = swoole_serialize::unpack($node);
            $this->areaId = $node['areaId'];
            $this->areaHash = $node['areaHash'];
            $this->nodeId = $node['nodeId'];
            $this->nodeHash = $node['nodeHash'];
        }
        return $this;
    }

    /**
     * 组合 area node client 获集群内唯一值
     * @param $sessionId
     * @param bool $polling
     * @return string
     */
    protected function encodeClientId($sessionId, $polling = false)
    {
        $this->initClientInfo();
        if ($polling) {
            return Swoole::packClientId($this->areaHash, $this->nodeHash, $sessionId, true);
        }
        return Swoole::packClientId($this->areaId, $this->nodeId, $sessionId);
    }

    /**
     * 由组合ID 反解析出 areaId nodeId clientId
     * @param $clientId
     * @return array
     */
    protected function decodeClientId($clientId)
    {
        return Swoole::unpackClientId($clientId);
    }

    /**
     * 是否为连接在当前节点的客户端, 参数 $client 为 decodeClientId 得到的数组
     * @param $client
     * @return bool
     */
    protected function ifClientInThisNode($client)
    {
        $this->initClientInfo();
        if ($client['polling']) {
            return $client['area'] === $this->areaHash && $client['node'] === $this->nodeHash;
        }
        return $client['area'] === $this->areaId && $client['node'] === $this->nodeId;
    }

    /**
     * 发消息给 Bus Server, 因 process->write 发送有长度限制, 根据实际情况一次性发送或分片发送
     * 一次性发送  0/workerId/command/messageNo/messageData
     * 分片发送
     *     -> 先发送消息头   1/workerId/command/messageNo/messageLen
     *     -> 再循环发送分片消息  2/workerId/chunkMessage
     *
     * @param $command
     * @param $data
     * @return mixed
     */
    protected function cmdBusServer($command, $data)
    {
        if (!isset($this->messagePacks[$command])) {
            $this->messagePacks[$command] = pack('n', $command);
        }
        $no = ++$this->messageNo;
        $this->messageReceive[$no] = false;
        $header = $this->messagePacks['workerId'].$this->messagePacks[$command].pack('N', $no);
        $dataLen = false;
        $isString = false;
        if (is_string($data)) {
            $isString = true;
            $dataLen = strlen($data);
        } elseif ($data instanceof Binaries || $data instanceof Binary) {
            $dataLen = $data->size();
        }
        if ($dataLen === false) {
            return false;
        }
        if ($dataLen < $this->chunkSize - 9) {
            // $data 长度在分片大小以内, 直接发过去
            $this->process->write('0'.$header.($isString ? $data : $data->buffer()));
        } else {
            // 分批次发送 (先发送消息头, 再发送消息实体)
            if ($isString) {
                $data = new Binary($data, true);
            }
            $this->process->write('1'.$header.pack('N', $data->size()));
            $data->setChunkSize($this->chunkSize - 3)->rewind();
            while ($data->valid()) {
                $this->process->write('2'.$this->messagePacks['workerId'].$data->current());
                $data->next();
            }
        }
        // 等待返回消息
        while(true) {
            $data = $this->channel->pop();
            return $data;
        }
        return null;
    }

    /**
     * 收到了 BusServer 回复的消息 messageNo/messageData
     * @param $message
     * @return BusContext
     */
    public function onReceiveMessage($message)
    {
        // 是否为直接 pipe 的消息
        $isPipe = (bool) substr($message, 0, 1);
        if ($isPipe) {
            return $this->onReceivePollingPipeMessage(substr($message, 1));
        }
        $messageNo = unpack('N', substr($message, 1, 4));
        $messageNo = $messageNo[1];
        $message = substr($message, 5);
        $this->messageReceive[$messageNo] = [$message];
        return $this->triggerReceiveMessage();
    }

    /**
     * 检查 messageReceive 是否按顺序收到了消息, 强制按照入栈消息的顺序出栈
     * 是 -> push 消息到 channel 中
     * 否 -> 继续等待
     * @return $this
     */
    protected function triggerReceiveMessage()
    {
        if (!count($this->messageReceive)) {
            $this->messageNo = 0;
            return $this;
        }
        reset($this->messageReceive);
        $message = current($this->messageReceive);
        if (!is_array($message)) {
            return $this;
        }
        $key = key($this->messageReceive);
        unset($this->messageReceive[$key]);
        $this->channel->push($message[0]);
        return $this->triggerReceiveMessage();
    }

    /**
     * 当前节点作为注册节点 -> 收到了一个新节点注册请求 -> 返回所有已注册节点数组
     * @param $address
     * @return array
     */
    public function registerNode($address)
    {
        $nodes = $this->cmdBusServer(Constants::NODE_REGISTER, $address);
        return swoole_serialize::unpack($nodes);
    }

    /**
     * 当前节点作为普通节点 -> 收到了节点变化通知
     * @param $nodes
     * @return mixed
     */
    public function addNode($nodes)
    {
        return $this->cmdBusServer(Constants::NODE_ADD, swoole_serialize::pack($nodes));
    }

    /**
     * 新客户端加入
     * @param int $sessionId
     * @param array $request  [query,header,cookie,server]
     * @param bool $polling
     * @return bool
     */
    public function openClient($sessionId, array $request = [], $polling = false)
    {
        $waitHeader = false;
        if (!$polling) {
            if (isset($request['header']['X-Custom-Header'])) {
               unset($request['header']['X-Custom-Header']);
            } else {
                $waitHeader = true;
            }
        }
        $rs = $this->addClient($sessionId, $request, $polling);
        if ($polling && $rs === 'none') {
            // polling 模式下, 当前 worker add, 但维护 eventSource 的不是当前 worker
            // BusServer 会通知维护 eventSource 的 worker 处理, 所以这里直接返回
            Swoole::debug('[BusContext]add user client(polling) has notice other worker');
            return false;
        }
        // 请求不包含了自定义 Header, 不认为打开成功, 等待通过 webSocket 发送自定义 headers
        if ($rs === 'yes' && $waitHeader) {
            Swoole::debug('[BusContext]wait webSocket send custom headers');
            $this->waitHeaders[$sessionId] = swoole_timer_after(3000, function () use ($sessionId) {
                Swoole::debug('[BusContext]wait webSocket custom headers timeout');
                unset($this->waitHeaders[$sessionId]);
                $this->openClientFailed($sessionId, static::TIMEOUT,static::TIMEOUT_REASON);
            });
            return false;
        }
        return $this->openClientCheck($rs, $sessionId, $polling);
    }

    /**
     * 新用户进入, 将用户信息推送给独立进程, sessionId 为当前节点唯一值
     * -> send (array)request[query,header,cookie,server]
     * <- Socket 客户端连接对象
     * @param $sessionId
     * @param array $request
     * @param bool $polling
     * @return string
     */
    protected function addClient($sessionId, array $request = [], $polling = false)
    {
        $request['sessionId'] = $sessionId;
        $request['polling'] = $polling;
        Swoole::debug('[BusContext]add user client'.($polling ? '(polling)' : ''));
        // sessionId 返回=> isPolling:fdHash, !isPolling:fdId
        $result = trim($this->cmdBusServer(Constants::CLIENT_ADD, swoole_serialize::pack($request)));
        Swoole::debug('[BusContext]add user client result:'.$result);
        return $result;
    }

    /**
     * 处理新客户端加入的返回结果
     * @param $result
     * @param $sessionId
     * @param bool $polling
     * @return bool
     */
    protected function openClientCheck($result, $sessionId, $polling = false)
    {
        if ($result !== 'yes') {
            $this->openClientFailed($sessionId, static::ADD, static::ADD_REASON, $polling);
            return false;
        }
        return $this->openClientSuccess($sessionId, $polling);
    }

    /**
     * 新客户端加入成功
     * @param $sessionId
     * @param bool $polling
     * @return Socket|array|bool
     */
    protected function openClientSuccess($sessionId, $polling = false)
    {
        Swoole::debug('[BusContext]use client open success, polling:'.(int) $polling);
        if (isset($this->waitHeaders[$sessionId])) {
            Swoole::debug('[BusContext]clear custom header wait timer');
            if ($polling) {
                $this->confirmRequestOpen($sessionId);
            } else {
                swoole_timer_clear($this->waitHeaders[$sessionId]);
                unset($this->waitHeaders[$sessionId]);
            }
        } elseif ($polling) {
            $this->openClientFailed($sessionId, static::CREATE, static::CREATE_REASON, $polling);
            return false;
        }
        $socket = $this->makeSocket($sessionId, [0], $polling);
        if (!($socket instanceof Socket)) {
            $this->openClientFailed($sessionId, static::CREATE, static::CREATE_REASON, $polling);
            return false;
        }
        if ($socket->msgType !== $socket::OPENED) {
            $this->openClientFailed($sessionId, static::GET, static::GET_REASON, $polling);
            return false;
        }
        return $this->triggerOpenListener($socket, $polling);
    }

    /**
     * 触发 onOpen 自定义回调
     * @param Socket $socket
     * @param bool $polling
     * @return bool
     */
    protected function triggerOpenListener(Socket $socket, $polling = false)
    {
        try {
            Swoole::debug('[BusContext]try to trigger open callback:'.$socket->clientId);
            call_user_func(Event::$onOpen, $socket);
            if ($polling) {
                $this->sendToEventSource($socket->clientFd, '0[]');
            } else {
                $this->sendMessageToWebSocket($socket->clientFd,'0['.$this->pingInterval.']');
            }
            return true;
        } catch (\Throwable $e) {
            $socket->setMessages('closed');
            if ($polling) {
                // polling open 抛出异常, 直接从内存进程中删除当前用户信息
                // 这样 eventSource 断开就不会回调 onClose 函数了
                $this->cmdBusServer(Constants::CLIENT_DROP, $socket->clientFd.'_1');
            }
            $this->closeUserClient([
                'polling' => $polling,
                'session' => $socket->clientFd,
                'code' => $e->closeCode ?? static::OPEN,
                'reason' => $e->getMessage()
            ]);
            return false;
        }
    }

    /**
     * 新客户端加入失败
     * @param $fd
     * @param $code
     * @param $message
     * @param bool $polling
     * @return $this
     */
    protected function openClientFailed($fd, $code, $message, $polling = false)
    {
        $this->closeUserClient([
            'session' => $fd,
            'code' => $code,
            'reason' => $message,
            'polling' => $polling,
        ]);
        Event::resolveException(new \ErrorException($message));
        return $this;
    }

    /**
     * 由 sessionId 创建一个 Socket 对象
     * @param $sessionId
     * @param array $messages
     * @param bool $polling
     * @return Socket
     */
    protected function makeSocket($sessionId, array $messages = [], $polling = false)
    {
        return new Socket($this->encodeClientId($sessionId, $polling), $this, $messages);
    }

    /**
     * 获取已进入用户信息
     * -> send nodeId_sessionId
     * <- [ areaId, nodeId, nodeAddress, workerId,  // 由 BusContext 添加
     *      clientArea, clientNodeId, clientFd,     // 由 BusServer 添加
     *      path, connectTime, client, groups, attribute, polling  // 内存中缓存信息
     *   ]
     * @param $clientId
     * @return array
     */
    public function getClient($clientId)
    {
        Swoole::debug('[BusContext]get client info:'.$clientId);
        $info = $this->cmdBusServer(Constants::CLIENT_GET, $clientId);
        $info = swoole_serialize::unpack($info);
        if (is_array($info)) {
            $this->initClientInfo();
            array_unshift($info, $this->areaId, $this->nodeId, $this->nodeAddress, $this->workerId);
        }
        return $info;
    }

    /**
     * 更新用户 group attributes 信息
     * @param $clientId
     * @param array $addGroup
     * @param array $subGroup
     * @param array $alterAttr
     * @return $this
     */
    public function updateClient($clientId, array $addGroup, array $subGroup, array $alterAttr)
    {
        Swoole::debug('[BusContext]update client info:'.$clientId);
        $this->cmdBusServer(Constants::CLIENT_UPDATE, swoole_serialize::pack([
            $clientId, $addGroup, $subGroup, $alterAttr
        ]));
        return $this;
    }

    /**
     * 获取指定 path 下所有用户 clientId
     * @param $path
     * @return array
     */
    public function getPathClient($path)
    {
        if (!($path = $this->checkStringArrayEmpty($path))) {
            return [];
        }
        $ids = $this->cmdBusServer(Constants::PATH_GET, swoole_serialize::pack([$path]));
        return $ids ? swoole_serialize::unpack($ids) : [];
    }

    /**
     * 获取指定 group 下所有用户 clientId
     * @param $group
     * @return array
     */
    public function getGroupClient($group)
    {
        if (!($group = $this->checkStringArrayEmpty($group))) {
            return [];
        }
        $ids = $this->cmdBusServer(Constants::GROUP_GET, swoole_serialize::pack([$group]));
        return $ids ? swoole_serialize::unpack($ids) : [];
    }

    /**
     * @param $arg
     * @return array
     */
    protected function checkStringArrayEmpty($arg)
    {
        $arr = [];
        $arg = is_array($arg) ? $arg : [$arg];
        foreach ($arg as $item) {
            if (is_string($item) && strlen($item)) {
                $arr[] = $item;
            }
        }
        return $arr;
    }

    /**
     * 主动关闭指定的客户端
     * 1. clientId 连接在当前节点 => 直接调用 closeUserClient 断开
     * 2. clientId 不在当前节点 => 发消息给 BusServer => 由 BusServer 转发消息给对应的节点要求关闭
     * @param $clientId
     * @param null $code
     * @param string $reason
     * @return $this
     */
    public function closeClient($clientId, $code = null, $reason = null)
    {
        if (!($client = $this->decodeClientId($clientId))) {
            return $this;
        }
        $inThisNode = $this->ifClientInThisNode($client);
        $client['code'] = $code;
        $client['reason'] = $reason;
        if ($inThisNode) {
            $this->closeUserClient($client);
        } else {
            Swoole::debug('[BusContext]close client in other node:'.$clientId);
            $this->cmdBusServer(Constants::CLIENT_CLOSE, swoole_serialize::pack($client));
        }
        return $this;
    }

    /**
     * 关闭连接在当前节点的 webSocket/polling 客户端
     * 1. closeClient 关闭当前节点指定的 client
     * 2. 收到其他节点发来的关闭要求, 关闭指定的 client
     * @param $client
     * @return $this
     */
    protected function closeUserClient($client)
    {
        if ($client['polling']) {
            Swoole::debug('[BusContext]close polling client in this node:'.$client['session']);
            $this->closePollingClient($client);
        } else {
            Swoole::debug('[BusContext]close webSocket client in this node:'.$client['session']);
            $this->closeWebSocketClient($client);
        }
        return $this;
    }

    /**
     * 关闭连接在当前节点的 webSocket 客户端
     * 1.发送 closeFrame 给客户端
     * 2.并关闭连接
     * 3.无需触发 onClose 回调, 关闭后会由上层自动触发
     * @param $client
     * @return $this
     */
    public function closeWebSocketClient($client)
    {
        if (!$this->swooleServer->exist($client['session'])) {
            return $this;
        }
        // webSocket 关闭帧
        $code = (int) $client['code'];
        if (($code < 3000 && $code != 1000) || $code > 4999) {
            $frame = chr(136) . chr(0);
        } else {
            $reason = substr($client['reason'], 0 ,123);
            $frame = chr(136) . chr(2 + strlen($reason)) . pack('n', $code) . $reason;
        }
        // 发送关闭帧 并 断开连接
        $this->swooleServer->send($client['session'], $frame);
        $this->swooleServer->close($client['session']);
        return $this;
    }

    /**
     * 关闭连接在当前节点的 polling 客户端
     * @param $client
     * @return $this
     */
    protected function closePollingClient($client)
    {
        $closed = false;
        $sessionId = $client['session'];
        if (isset($this->waitHeaders[$sessionId])) {
            // 此刻还未收到认证信息
            list($timer, $response) = $this->waitHeaders[$sessionId];
            swoole_timer_clear($timer);
            $this->closeEventSource($response, $client['code'], $client['reason'], $sessionId);
            unset($this->waitHeaders[$sessionId]);
            $closed = true;
        }
        $response = $this->clearEventSourceTimer($client['session']);
        if ($response) {
            unset($this->pollingResponses[$client['session']]);
            if (!$closed) {
                $this->closeEventSource($response, $client['code'], $client['reason'], $sessionId);
            }
        }
        return $this;
    }

    /**
     * 广播消息给指定范围的客户端 (包括连接当前节点的客户端)
     * @param $scopeType
     * @param $scope
     * @param array $data
     * @param string $exSession 要排除的 session, 即当前发送广播的用户 session
     * @return $this
     */
    public function broadcast($scopeType, $scope, array $data = [], $exSession = null)
    {
        $currentScope = $forwardScope = $scope;
        if ($scopeType === Constants::FORWARD_CLIENT) {
            // 转发到指定的 clientIds, 只有一个可直接调用 emit 函数
            if (is_string($scope)) {
                return $this->emit($scope, $data);
            } elseif (!is_array($scope)) {
                throw new \InvalidArgumentException('Invalid clientIds for emit');
            }
            $currentScope = $forwardScope = [];
            foreach ($scope as $clientId) {
                $client = $this->decodeClientId($clientId);
                if ($this->ifClientInThisNode($client)) {
                    $currentScope[$client['session']] = $client['polling'];
                } else {
                    $forwardScope[] = $client;
                }
            }
            if (!$currentScope && !$forwardScope) {
                return $this;
            }
        }
        $code = '3';
        $binaries = [];
        $data = $this->encodeMessage($this->checkEmitData($data), $binaries);
        // 发给当前节点的客户端
        if ($scopeType === Constants::FORWARD_WORLD || $currentScope) {
            $this->emitToClient($scopeType, $currentScope, $code, $data, $binaries, $exSession);
        }
        // 转发给其他节点
        if ($scopeType === Constants::FORWARD_WORLD || $forwardScope) {
            $data = $code.(($count = count($binaries)) ? '-'.$count : '').json_encode($data);
            $this->broadcastMessage($scopeType, $forwardScope, $data, $binaries);
        }
        return $this;
    }

    /**
     * 发消息给指定的普通客户端
     * @param string $clientId
     * @param array $data
     * @param bool $replyCode
     * @return $this
     */
    public function emit($clientId, array $data = [], $replyCode = false)
    {
        return $this->emitMessage($clientId, $this->checkEmitData($data), $replyCode);
    }

    /**
     * 检测 data, 至少有一项, 且为字符串
     * @param array $data
     * @return array
     */
    protected function checkEmitData(array $data = [])
    {
        if (!count($data) || !is_string($data[0])) {
            throw new \InvalidArgumentException('Invalid event for emit');
        }
        return $data;
    }

    /**
     * 发消息给指定的普通客户端
     * @param string $clientId 全局唯一客户端id
     * @param array $data
     * @param bool $replyCode (false:发给指定的clientId, true:当前用户的clientId, int:当前用户且是回复消息)
     * @return $this
     */
    protected function emitMessage($clientId, array $data = [], $replyCode = false)
    {
        // 消息 code (3:主动发送, 4xx:回复消息)
        if (!is_string($clientId) || !($client = $this->decodeClientId($clientId))) {
            throw new \InvalidArgumentException('Invalid clientId for emit');
        }
        // 只有 clientId 连接在当前节点, 且是自己发给自己的情况, 才允许最后一个参数是回调函数
        $needReply = false;
        if (is_callable(end($data))) {
            if ($replyCode !== true) {
                throw new \InvalidArgumentException('Emit last argument can not be callable');
            }
            $needReply = true;
        }
        // 处理待发送消息,提取消息中的二进制变量
        $inThisNode = !($replyCode === false) || $this->ifClientInThisNode($client);
        $binaries = [];
        $data = $this->encodeMessage($data, $binaries);
        $code = $replyCode === false || $replyCode === true ? '3' : '4'.$replyCode;
        if ($needReply) {
            // code 设置为 3xx, 需要客户端回复
            $code .= $this->addReplyListener($client['session'], array_pop($data));
        }
        $message = $code.(($count = count($binaries)) ? '-'.$count : '').json_encode($data);
        if (!$inThisNode) {
            // 不在当前节点, 通知其他节点转发消息
            $this->broadcastMessage(Constants::FORWARD_CLIENT, [$client], $message, $binaries);
        } elseif ($client['polling'] && $replyCode === false) {
            // pooling 客户端, 在当前节点, 但不确定是否在当前 worker
            $this->emitToClient(Constants::FORWARD_CLIENT, [$client['session'] => 1], $code, $data, $binaries);
        } else {
            // 在当前节点, 且在当前 worker
            $this->sendMessageToClient($client['session'], $message, $binaries, $client['polling']);
        }
        return $this;
    }

    /**
     * 广播消息给那些没连接当前节点的客户端, 将 txt binary 消息统一打包发送给 BusServer
     * @param int $type 广播类型
     * @param mixed $scope 广播范围
     * @param string $data 广播 text 消息
     * @param array $binaries 广播 binary 消息
     * @return bool
     */
    protected function broadcastMessage($type, $scope, $data, array $binaries = [])
    {
        if ($message = Buffer::packBroadcastMessage($type, $scope, $data, $binaries)) {
            $this->cmdBusServer(Constants::FORWARD, $message);
            return true;
        }
        return false;
    }

    /**
     * 发送消息给指定客户端(该客户端连接在当前节点), 添加一个该客户端回复消息的回调函数
     * @param $fd
     * @param callable $callback
     * @return int|mixed
     */
    protected function addReplyListener($fd, callable $callback)
    {
        if (!isset($this->replyListeners[$fd])) {
            $this->replyListeners[$fd] = [];
        }
        if (count($this->replyListeners[$fd])) {
            end($this->replyListeners[$fd]);
            $key = key($this->replyListeners[$fd]) + 1;
        } else {
            $key = 0;
        }
        $this->replyListeners[$fd][$key] = $callback;
        return $key;
    }

    /**
     * 从待发送消息中提取二进制消息
     * @param array $args
     * @param Binary[]|bool $binary
     * @return bool|array
     */
    protected function encodeMessage(array $args = [], &$binary = [])
    {
        try {
            $args = $this->encodeMessageBin($args, $binary);
        } catch (\ErrorException $e) {
            return false;
        }
        return $args;
    }

    /**
     * 循环处理要发送消息
     * @param $args
     * @param Binary[] $binary
     * @return array
     * @throws \ErrorException
     */
    protected function encodeMessageBin($args, &$binary = [])
    {
        $newArg = [];
        foreach ($args as $key => $arg) {
            if ($arg instanceof Binary) {
                $newArg[$key] = ['_bin_' => 1, '_num_' => count($binary)];
                $binary[] = $arg;
            } elseif (is_array($arg)) {
                $newArg[$key] = $this->encodeMessageBin($arg, $binary);
            } else {
                $newArg[$key] = $arg;
            }
        }
        return $newArg;
    }

    /**
     * 处理普通客户端通过 http polling 请求
     * ----------------------------------------------------------------------------------------
     * @param swoole_http_request $request
     * @param swoole_http_response $response
     * @return $this
     */
    public function resolvePollingRequest(swoole_http_request $request, swoole_http_response $response)
    {
        $method = strtoupper($request->server['REQUEST_METHOD']);
        // 若跨域, 客户端可能会先行询问
        if ($method === 'OPTIONS') {
            $response->status(200);
            $response->header('Access-Control-Allow-Origin', '*');
            $response->header('Access-Control-Allow-Methods', 'GET, POST');
            $response->end();
            return $this;
        }
        // eventSource 请求建立单向通道
        if ($method === 'GET') {
            return $this->onEventConnect($request, $response);
        }
        // ajax 短连接请求 (打开|消息|关闭)
        # post 请求: 基本信息验证
        $clientId = isset($request->get['id']) ? trim($request->get['id']) : null;
        if (!($client = $this->decodeClientId($clientId))) {
            $clientId = null;
        }
        $action = $payload = $postOpen = $customHeader = null;
        if ($clientId) {
            // form post 方式 open
            if (isset($request->post['_action']) && $request->post['_action'] === 'open') {
                $action = 'open';
                $postOpen = true;
                $customHeader = $request->post;
                unset($customHeader['_action']);
            } else {
                $payload = $method === 'POST' ? $request->rawContent() : null;
                if (strlen($payload) > 3) {
                    $action = substr($payload, 0, 4);
                    $payload = substr($payload, 4);
                    if ($action === 'open') {
                        $customHeader = json_decode($payload, true);
                    }
                }
            }
        }
        if (!$action || !in_array($action, ['open', 'push', 'biny', 'clos'])) {
            Swoole::debug('[BusContext]user push error:'.$request->fd);
            $response->status(403);
            $response->end();
            return $this;
        }
        // 客户端打开
        if ($action === 'open') {
            $user = [
                'query' => $request->get,
                'header' => $request->header,
                'server' => $request->server,
                'cookie' => $request->cookie
            ];
            if ($customHeader) {
                $customHeader = array_combine(array_map(function($key) {
                    return join('-', array_map('ucfirst', explode('-', strtolower($key))));
                }, array_keys($customHeader)), $customHeader);
                $user['header'] = array_merge($user['header'], $customHeader);
            }
            unset($user['query']['id']);
            return $this->onRequestOpen($request, $response, $client, $user, $postOpen);
        }
        // 客户端断开
        if ($action === 'clos') {
            return $this->onRequestClose($response, $client, $payload);
        }
        // 客户端消息
        $client['clientId'] = $clientId;
        return $this->onRequestMessage($response, $client, $payload, $action === 'biny');
    }

    /**
     * 处理连接在当前节点的 eventSource 断开 (意外断开, 客户端主动断开, 服务端主动关闭)
     * @param $fd
     * @return $this
     */
    public function resolvePollingClose($fd)
    {
        Swoole::debug('[BusContext]client EventSource disconnect:'.$fd);
        $clientInfo = $this->cmdBusServer(Constants::CLIENT_EVENT_CLOSE, (string) $fd);
        $clientInfo = swoole_serialize::unpack($clientInfo);
        if ($clientInfo) {
            Swoole::debug('[BusContext]remove eventClient success(reply):'.$fd);
            $this->onEventSourceClose($clientInfo);
        }
        return $this;
    }

    /**
     * 处理 BusServer 主动 pipe 来的内部消息, 发生场景以及原因
     * polling 客户端主动发送消息上来 / 服务端主动要发消息给客户端
     * 因为 swoole 要挂起 http 请求, 就必须保持 response 对象, 而 response 对象只能保持在 worker 内
     * 那么服务端需要写入消息到 eventSource 中, 就必须在保持 eventSource response 对象的 worker 上执行
     * 所以即使是同节点服务器, 不能确定是否为 eventSource worker 的, 都要讲命令交给 BusServer
     * BusServer 维护了 sessionId workerId 的映射关系, 主动 pipe 命令消息给 worker, 该函数就是用来接收此类消息的
     * @param $message
     * @return $this
     */
    protected function onReceivePollingPipeMessage($message)
    {
        $command = unpack('n', substr($message, 0, 2));
        $command = is_array($command) && isset($command[1]) ? $command[1] : null;
        $message = substr($message, 2);
        switch ($command) {
            case Constants::CLIENT_EVENT_CLOSE:
                // 服务端主动关闭
                Swoole::debug('[BusContext]eventClient close pipe message');
                $this->onEventSourceClose(swoole_serialize::unpack($message));
                break;
            case Constants::CLIENT_EVENT_EMIT:
                // 服务端主动发消息
                Swoole::debug('[BusContext]eventClient event emit pipe message');
                $emit = swoole_serialize::unpack($message);
                $this->sendMessageToClient($emit['clients'], $emit['data'], $emit['binaries'], true);
                break;
            case Constants::CLIENT_REQUEST_OPEN:
                // 客户端 open 请求
                Swoole::debug('[BusContext]eventClient request open pipe message');
                $message = swoole_serialize::unpack($message);
                $this->openClientCheck($message['result'], $message['sessionId'], true);
                break;
            case Constants::CLIENT_REQUEST_CLOSE:
                // 客户端 close 请求
                Swoole::debug('[BusContext]eventClient request close pipe message');
                $this->closePollingClient(swoole_serialize::unpack($message));
                break;
            case Constants::CLIENT_REQUEST_EMIT:
                // 客户端主动发送的消息
                Swoole::debug('[BusContext]eventClient request emit pipe message');
                $this->triggerRequestMessage(swoole_serialize::unpack($message));
                break;
        }
        return $this;
    }

    /**
     * 处理 polling 客户端 eventSource 连接请求
     * @param swoole_http_request $request
     * @param swoole_http_response $response
     * @return $this
     */
    protected function onEventConnect(swoole_http_request $request, swoole_http_response $response)
    {
        if (!isset($request->get['polling']) || $request->get['polling'] !== 'polling') {
            $response->status(403);
            $response->end();
            return $this;
        }
        Swoole::debug('[BusContext]client EventSource connect:'.$request->fd);
        $sessionId = $this->cmdBusServer(Constants::CLIENT_EVENT_CONNECT, (string) $request->fd);
        $response->header('Access-Control-Allow-Origin', '*');
        $response->header('Content-Type', 'text/event-stream');
        $response->header('Cache-Control', 'no-cache');
        $response->header('Connection', 'keep-alive');
        $response->write(':'.str_repeat(' ', 2048)."\n\n");
        $response->write("retry: 1000\n\n");
        if ($sessionId === 'fail') {
            Swoole::debug('[BusContext]client EventSource connect failed:'.$request->fd);
            $response->write("event: failed\ndata: server\n\n");
            $response->end();
            $this->swooleServer->close($request->fd);
            return $this;
        }
        return $this->waitRequestOpen($sessionId, $response);
    }

    /**
     * 当前 worker 上维护的 eventSource 关闭了 (若 opened=1 说明当前用户信息还在 BusServer, 调用自定义 onClose 函数)
     * @param $clientInfo
     * @return $this
     */
    protected function onEventSourceClose($clientInfo)
    {
        if (!is_array($clientInfo) || !isset($clientInfo['sessionId'])) {
            return $this;
        }
        if ($this->clearEventSourceTimer($clientInfo['sessionId'])) {
            unset($this->pollingResponses[$clientInfo['sessionId']]);
        }
        if (!isset($clientInfo['opened']) || !$clientInfo['opened']) {
            return $this;
        }
        return $this->triggerCloseListener($clientInfo['sessionId'],null, null, true);
    }

    /**
     * 处理 polling 客户端 post open 请求
     * @param swoole_http_request $request
     * @param swoole_http_response $response
     * @param $client
     * @param $user
     * @param $postOpen
     * @return $this
     */
    protected function onRequestOpen(swoole_http_request $request, swoole_http_response $response, $client, $user, $postOpen)
    {
        // eventSource 在当前节点, 直接执行 open
        if ($this->ifClientInThisNode($client)) {
            Swoole::debug('[BusContext]polling request open in this node:'.$client['session']);
            $this->openClient($client['session'], $user, true);
        } else {
            Swoole::debug('[BusContext]polling request open in other node:'.$client['session']);
            $this->cmdBusServer(Constants::CLIENT_REQUEST_OPEN, swoole_serialize::pack(compact('client','user')));
        }
        $origin = null;
        if (!$postOpen) {
            if (isset($request->header['Origin'])) {
                $origin = $request->header['Origin'];
            } elseif (isset($request->header['Referer'])) {
                $referer = parse_url($request->header['Referer']);
                if (isset($referer['scheme']) && isset($referer['host'])) {
                    $origin = $referer['scheme'].'://'.$referer['host'];
                }
            }
            if ($origin) {
                $response->header('Access-Control-Allow-Credentials', 'true');
            }
        }
        $response->header('Access-Control-Allow-Origin', $origin ?: '*');
        $response->end('ok');
        return $this;
    }

    /**
     * 处理 polling 客户端 post emit 请求
     * @param swoole_http_response $response
     * @param $client
     * @param $payload
     * @param bool $binary
     * @return $this
     */
    protected function onRequestMessage(swoole_http_response $response, $client, $payload, $binary = false)
    {
        // 交给维护当前 client eventSource 的对应节点 worker 处理, 在当前 worker 直接处理会大大增加通信复杂度
        // 因为 onMessage 回调中可能会发送消息给客户端, 最终还是要转交给 eventSource 节点, 干脆这里直接转交
        Swoole::debug('[BusContext]polling request emit:'.$client['session']);
        $payload = ($binary ? '1' : '0').$payload;
        $this->cmdBusServer(
            Constants::CLIENT_REQUEST_EMIT,
            swoole_serialize::pack(compact('client', 'payload'))
        );
        $response->header('Access-Control-Allow-Origin', '*');
        $response->end();
        return $this;
    }

    /**
     * 连接在当前节点的 eventSource 客户端 通过 request post 发来了消息
     * polling 客户度 request pose emit 请求 发给了 BusServer => 转发到当前 worker 了
     * request = [client => array, payload => string]
     * @param $request
     * @return $this
     */
    protected function triggerRequestMessage($request)
    {
        $client = $request['client'];
        if (!isset($this->pollingResponses[$client['session']])) {
            return $this;
        }
        $payload = $request['payload'];
        $isBinary = (bool) substr($payload, 0, 1);
        $payload = substr($payload, 1);
        $binaries = [];
        if ($isBinary) {
            // 处理二进制消息 [bufferCount/[buffer_len...]/[buffer...]]
            $payloadLen = strlen($payload);
            if ($payloadLen < 4 || !($binaryLen = unpack('l', substr($payload, 0, 4)))) {
                return $this;
            }
            $binaryLen = $binaryLen[1];
            $headerLen = $binaryLen ? ($binaryLen + 1) * 4 : 0;
            if (!$headerLen || $payloadLen < $headerLen || !($binaryLen = unpack('l*', substr($payload, 4, $binaryLen * 4)))) {
                return $this;
            }
            if ($payloadLen < $headerLen + array_sum($binaryLen)) {
                return $this;
            }
            foreach ($binaryLen as $len) {
                $binaries[] = substr($payload, $headerLen, $len);
                $headerLen += $len;
            }
            $payload = array_shift($binaries);
        }
        if (false === ($code = $this->parseMessageDataCode($payload, $data))) {
            return $this;
        }
        return $this->triggerMessageListener($client['clientId'], $code, $data, $binaries);
    }

    /**
     * 处理 polling 客户端 post close 请求
     * @param swoole_http_response $response
     * @param $client
     * @param $close
     * @return $this
     */
    protected function onRequestClose(swoole_http_response $response, $client, $close)
    {
        Swoole::debug('[BusContext]polling request close:'.$client['session']);
        $close = json_decode($close, true);
        $this->cmdBusServer(
            Constants::CLIENT_REQUEST_CLOSE,
            swoole_serialize::pack(compact('client', 'close'))
        );
        $response->header('Access-Control-Allow-Origin', '*');
        $response->end();
        return $this;
    }

    /**
     * eventSource 连接成功后, 等待 request 发送 open 请求
     * @param $sessionId
     * @param swoole_http_response $response
     * @return $this
     */
    protected function waitRequestOpen($sessionId, swoole_http_response $response)
    {
        $timer = swoole_timer_after(5000, function () use ($response, $sessionId) {
            Swoole::debug('[BusContext]wait polling open timeout:'.$sessionId);
            unset($this->waitHeaders[$sessionId]);
            $this->closeEventSource($response, static::TIMEOUT, static::TIMEOUT_REASON);
        });
        $this->waitHeaders[$sessionId] = [$timer, $response];
        $clientId = $this->encodeClientId($sessionId, true);
        Swoole::debug('[BusContext]client EventSource connect success:'.$clientId);
        $response->write("event: success\ndata: $clientId\n\n");
        return $this;
    }

    /**
     * 已经成功接收到客户端的 request open, 并将客户端信息并存储到内存进程之后
     * @param $sessionId
     * @return $this
     */
    protected function confirmRequestOpen($sessionId)
    {
        list($timer, $response) = $this->waitHeaders[$sessionId];
        swoole_timer_clear($timer);
        unset($this->waitHeaders[$sessionId]);
        $response->timer = 0;
        $this->pollingResponses[$sessionId] = $response;
        return $this->sendToEventSource($sessionId);
    }

    /**
     * 发消息给连接在当前节点的 eventSource
     * @param $sessionId
     * @param $message
     * @param null $event
     * @return $this
     */
    protected function sendToEventSource($sessionId, $message = null, $event = null)
    {
        if (!($response = $this->clearEventSourceTimer($sessionId))) {
            Swoole::debug('[BusContext]send message to none exist eventSource:'.$sessionId);
            return $this;
        }
        if ($message) {
            Swoole::debug('[BusContext]send message to eventSource:'.$sessionId);
            $message = ($event ? "event: $event\n" : '')."data: $message\n\n";
        } else {
            $message = "data: 1\n\n";
        }
        if ($response->write($message)) {
            $response->timer = swoole_timer_after($this->pingInterval, function () use ($sessionId, $response) {
                $response->timer = 0;
                $this->sendToEventSource($sessionId);
            });
        } else {
            unset($this->pollingResponses[$sessionId]);
        }
        return $this;
    }

    /**
     * 清除 fd 的 ping 计时器, 并获取 response 对象
     * @param $sessionId
     * @return bool|swoole_http_response
     */
    protected function clearEventSourceTimer($sessionId)
    {
        if (!isset($this->pollingResponses[$sessionId])) {
            return false;
        }
        $response = $this->pollingResponses[$sessionId];
        if ($response->timer) {
            swoole_timer_clear($response->timer);
            $response->timer = 0;
        }
        return $response;
    }

    /**
     * 发送关闭帧给 polling eventSource
     * @param swoole_http_response $response
     * @param null $code
     * @param string $reason
     * @param bool $sessionId
     * @return $this
     */
    protected function closeEventSource(swoole_http_response $response, $code = null, $reason = '', $sessionId = false)
    {
        $code = (int) $code;
        if (($code < 3000 && $code != 1000) || $code > 4999) {
            $data = [];
        } else {
            $data = ['code' => $code, 'reason' => substr($reason, 0 ,123)];
        }
        if ($sessionId) {
            $this->triggerCloseListener($sessionId, $data['code'], $data['reason'], true);
        }
        $data = json_encode($data);
        $response->write("event: offline\ndata: $data\n\n");
        $response->end();
        $this->swooleServer->close($response->fd);
        return $this;
    }

    /**
     * 处理普通客户端通过 webSocket 发来的消息 (客户端肯定是连接在当前节点的, 不然也不会收到)
     * ----------------------------------------------------------------------------------------
     * @param swoole_websocket_frame $frame
     * @return bool
     */
    public function resolveWebSocketMessage(swoole_websocket_frame $frame)
    {
        if ($frame->opcode === WEBSOCKET_OPCODE_BINARY) {
            $data = $this->waiteReceiveBinary($frame);
        } else {
            $data = $this->waiteReceiveMessage($frame);
        }
        if (!$data) {
            return false;
        }
        list($code, $data, $binaries) = $data;
        // 模拟心跳
        if ($code === 1) {
            return false;
        }
        // 补发的自定义 Header
        if ($code === 9) {
            $data = count($data) ? $data[0] : null;
            $data = is_array($data) && count($data) ? $data : null;
            if ($data) {
                Swoole::debug('[BusContext]update user custom header:'.$frame->fd);
                $this->cmdBusServer(Constants::CLIENT_UPHEADER, swoole_serialize::pack([
                    'sessionId' => $frame->fd,
                    'headers' => $data
                ]));
            } else {
                Swoole::debug('[BusContext]receive empty custom header:'.$frame->fd);
            }
            $this->openClientSuccess($frame->fd);
            return true;
        }
        $this->triggerMessageListener($this->encodeClientId($frame->fd), $code, $data, $binaries);
        return false;
    }

    /**
     * webSocket 客户端关闭后的回调函数
     * 该方法被触发的可能性
     * 1. 当前节点的 webSocket 客户端关闭(意外关闭或正常close) => 触发 swoole onClose 回调 => 触发该函数
     * 2. 当前节点服务端主动关闭客户端 => 触发 swoole onClose 回调 => 触发该函数
     * 3. 其他节点要关闭当前节点的客户端 => 其他节点发消息给当前节点告知要关闭的 clientID =>
     *    当前节点主动关闭clientId的客户端 => 触发 swoole onClose 回调 => 触发该函数
     * @param $sessionId
     * @param null $code
     * @param string $reason
     * @param bool $error
     * @return $this
     */
    public function resolveWebSocketClose($sessionId, $code = null, $reason = '', $error = false)
    {
        if ($error) {
            // 在未 open 成功就发生异常而关闭的, 不触发自定义的 onClose 回调, 仅从内存中删除当前用户信息
            Swoole::debug('[BusContext]client closed via error:'.$sessionId);
            $this->cmdBusServer(Constants::CLIENT_DROP, $sessionId.'_0');
            return $this;
        }
        return $this->triggerCloseListener($sessionId, $code, $reason);
    }

    /**
     * 客户端 (webSocket/eventSource 一定是连接在当前节点且当前worker) 发来消息, 触发自定义 onMessage 回调
     * @param $clientId
     * @param $code
     * @param array $data
     * @param array $binaries
     * @return $this
     */
    protected function triggerMessageListener($clientId, $code, array $data, array $binaries = [])
    {
        $data = $this->decodeReceiveMessage($data, $binaries);
        $socket = new Socket($clientId, $this, array_merge([$code], $data));
        $resolved = false;
        if ($socket->msgType === $socket::MESSAGE) {
            Swoole::debug('[BusContext]call message listener');
            $resolved = $this->callMessageListener($socket);
        } elseif ($socket->msgType === $socket::REPLY) {
            Swoole::debug('[BusContext]call reply listener');
            $resolved = $this->callReplyListener($socket);
        }
        if (!$resolved) {
            Event::resolveException(new \InvalidArgumentException('Invalid client message'), $socket);
        }
        return $this;
    }

    /**
     * 将普通客户端发送来的 二进制消息 合并进 对应文本消息, 返回最终数组
     * @param array $data
     * @param array $binaries
     * @return array
     */
    protected function decodeReceiveMessage(array $data, array $binaries)
    {
        foreach ($data as $key => $item) {
            if (!is_array($item)) {
                continue;
            }
            if (array_keys($item) === ['_bin_', '_num_']) {
                if (!isset($binaries[$item['_num_']])) {
                    $data[$key] = null;
                } else {
                    $data[$key] = $binaries[$item['_num_']];
                }
            } else {
                $data[$key] = $this->decodeReceiveMessage($item, $binaries);
            }
        }
        return $data;
    }

    /**
     * 响应客户端主动发送的消息
     * @param Socket $socket
     * @return bool
     */
    protected function callMessageListener(Socket $socket)
    {
        $messages = $socket->messages;
        $event = count($messages) ? $messages[0] : null;
        if ($event && is_string($event) && isset(Event::$listeners[$event])) {
            array_shift($messages);
            $socket->setMessages($messages);
            array_unshift($messages, $socket);
            foreach (Event::$listeners[$event] as $listener) {
                Event::callListener($listener, $messages, $socket);
            }
        } else {
            Event::callListener(Event::$onMessage, [$socket], $socket);
        }
        return true;
    }

    /**
     * 响应客户端被动回复的消息
     * @param Socket $socket
     * @return bool
     */
    protected function callReplyListener(Socket $socket)
    {
        $fd = $socket->clientFd;
        $key = $socket->_headerCode;
        if ($key === false || !isset($this->replyListeners[$fd]) || !isset($this->replyListeners[$fd][$key])) {
            return false;
        }
        $data = $socket->messages;
        array_unshift($data, $socket);
        Event::callListener($this->replyListeners[$fd][$key], $data, $socket);
        unset($this->replyListeners[$fd][$key]);
        return true;
    }

    /**
     * 客户端 (webSocket/eventSource 一定是连接在当前节点,当前worker) 关闭, 触发自定义 onClose 回调
     * @param $sessionId
     * @param null $code
     * @param string $reason
     * @param bool $polling
     * @return $this
     */
    protected function triggerCloseListener($sessionId, $code = null, $reason = '', $polling = false)
    {
        $socket = $this->makeSocket($sessionId, [8], $polling);
        Swoole::debug('[BusContext]client closed:'.$sessionId);
        if ($socket->msgType === $socket::CLOSED) {
            Event::callListener(Event::$onClose, [$socket, $code, $reason], $socket);
            $this->cmdBusServer(Constants::CLIENT_DROP, $sessionId.'_'.(int) $polling);
        }
        return $this;
    }

    /**
     * 处理 集群内节点 通过 webSocket 通道发来的消息
     * ----------------------------------------------------------------------------------------
     * @param swoole_websocket_frame $frame
     * @return $this
     */
    public function resolveNodeMessage(swoole_websocket_frame $frame)
    {
        // 二进制消息, 肯定是转发消息的命令
        if ($frame->opcode === WEBSOCKET_OPCODE_BINARY) {
            Swoole::debug('[BusContext]receive binary from other node');
            $data = $this->waiteReceiveBinary($frame);
            if ($data && isset($this->messageEmit[$frame->fd])) {
                $this->emitToClient($this->messageEmit[$frame->fd][0], $this->messageEmit[$frame->fd][1], $data[0], $data[1], $data[2]);
                unset($this->messageEmit[$frame->fd]);
            }
            return $this;
        }
        $fd = $frame->fd;
        $data = $frame->data;
        if ($data === 'open') {
            // do nothing
            Swoole::debug('[BusContext]other node open webSocket as client');
            return $this;
        }
        $needReply = (bool) substr($data, 0, 1);
        if ($needReply) {
            $block = substr($data, 1, 6);
            $header = unpack('nWorkerId/nMessageNo/nCommand', $block);
            // 消息头错误
            if (count($header) !== 3) {
                return $this;
            }
            $command = $header['Command'];
            $message = substr($data, 7);
        } else {
            $block = substr($data, 1, 2);
            $header = unpack('n', $block);
            // 消息头错误
            if (!count($header)) {
                return $this;
            }
            $command = $header[1];
            $message = substr($data, 3);
        }
        Swoole::debug('[BusContext]receive command from other node, command:'.$command);
        $reply = $this->socketDefaultMessage;
        switch ($command) {
            // 当前节点连接了 eventSource, open 操作发送到了其他节点, 其他节点将 open 信息转发过来了
            case Constants::CLIENT_REQUEST_OPEN:
                $message = swoole_serialize::unpack($message);
                $this->openClient($message['sessionId'], $message['user'], true);
                break;
            // 当前节点连接了 eventSource, close emit 操作发送到了其他节点, 其他节点将信息转发过来了
            case Constants::CLIENT_REQUEST_EMIT:
            case Constants::CLIENT_REQUEST_CLOSE:
                $this->cmdBusServer($command, $message);
                break;
            // (读取/更新)用户信息, 获取指定(path|group)下的客户端ID
            case Constants::CLIENT_GET:
            case Constants::CLIENT_UPDATE:
            case Constants::PATH_GET:
            case Constants::GROUP_GET:
                $reply = $this->cmdBusServer($command, $message);
                break;
            // 关闭当前节点的指定客户端
            case Constants::CLIENT_CLOSE:
                $needReply = false;
                $client = swoole_serialize::unpack($message);
                $this->closeUserClient($client);
                break;
            // 群发消息给当前节点的客户端
            case Constants::FORWARD:
                $needReply = false;
                // [$type, $scope, $message]
                $emits = Buffer::unpackBroadcastMessage($message);
                if (!$emits) {
                    return $this;
                }
                $frame->data = array_pop($emits);
                // [$code, $data, $binaries]
                $data = $this->waiteReceiveMessage($frame);
                if (!$data) {
                    $this->messageEmit[$frame->fd] = $emits;
                    return $this;
                }
                $this->emitToClient($emits[0], $emits[1], $data[0], $data[1], $data[2]);
                break;
        }
        if ($needReply) {
            Swoole::debug('[BusContext]reply to other node, command:'.$command);
            $this->swooleServer->push($fd, $block.$reply);
        }
        return $this;
    }

    /**
     * 收到客户端（集群节点 或 普通客户端）发来消息 最终返回 false || [code, (array) data, (array) binaries]
     * @param swoole_websocket_frame $frame
     * @return mixed
     */
    protected function waiteReceiveMessage(swoole_websocket_frame $frame)
    {
        // 模拟心跳
        if ($frame->data === '1') {
            return [1, [], []];
        }
        if (false === ($code = $this->parseMessageDataCode($frame->data, $data))) {
            return false;
        }
        // 含有二进制数据, 缓存消息, 等待所需二进制消息
        if (strpos($code, '-')) {
            list($code, $count) = explode('-', $code);
            $this->messages[$frame->fd] = array_merge([(int) $count, (int) $code], $data);
            return false;
        }
        return [(int) $code, $data, []];
    }

    /**
     * 收到客户端（集群节点 或 普通客户端）发来消息 最终返回 false || [code, (array) data, (array) binaries]
     * @param swoole_websocket_frame $frame
     * @return mixed
     */
    protected function waiteReceiveBinary(swoole_websocket_frame $frame)
    {
        $fd = $frame->fd;
        if (!isset($this->messages[$fd])) {
            return false;
        }
        $count = $this->messages[$fd][0];
        if (!isset($this->messageBuffer[$fd])) {
            $this->messageBuffer[$fd] = [new Binary($frame->data, true)];
            $ready = 1;
        } else {
            $this->messageBuffer[$fd][] = new Binary($frame->data, true);
            $ready = count($this->messageBuffer[$fd]);
        }
        if ($ready === $count) {
            array_shift($this->messages[$fd]); // shift count
            $code = array_shift($this->messages[$fd]);
            $data = $this->messages[$fd];
            $binaries = $this->messageBuffer[$fd];
            unset($this->messages[$fd], $this->messageBuffer[$fd]);
            return [$code, $data, $binaries];
        }
        return false;
    }

    /**
     * 解析收到 message 中的 code
     * @param $message
     * @param null $data
     * @return bool|string
     */
    protected function parseMessageDataCode($message, &$data = null)
    {
        $code = false;
        if ($pos = strpos($message, '[')) {
            $code = substr($message, 0, $pos);
            $data = json_decode(substr($message, $pos), true);
        }
        if ($code === false || !$data) {
            return false;
        }
        return $code;
    }

    /**
     * 其他节点或当前发出的命令, 要发送消息到指定范围内的当前节点客户端(们)
     * @param $scopeType
     * @param $scope
     * @param $code
     * @param array $data
     * @param array $binaries
     * @param string $exSession
     * @return $this
     */
    protected function emitToClient($scopeType, $scope, $code, array $data = [], array $binaries = [], $exSession = null)
    {
        Swoole::debug('[BusContext]forward message from other node:'.$scopeType);
        $webSocketSessions = $pollingSessions = [];
        if ($scopeType === Constants::FORWARD_CLIENT) {
            $clients = $scope;
        } else {
            // 群发到当前节点的客户端, 根据范围查找所有 sessionId
            $clients = $this->cmdBusServer(
                Constants::EMIT_CLIENTS,
                swoole_serialize::pack(compact('scopeType', 'scope', 'exSession'))
            );
            $clients = swoole_serialize::unpack($clients);
        }
        // $client 格式为 [sessionId => (bool) $polling, ....]
        if (is_array($clients)) {
            foreach ($clients as $sessionId => $polling) {
                if ($polling) {
                    $pollingSessions[] = $sessionId;
                } else {
                    $webSocketSessions[] = $sessionId;
                }
            }
        }
        $message = $code.(($count = count($binaries)) ? '-'.$count : '').json_encode($data);
        if (count($pollingSessions)) {
            // polling 客户端, 不确定是否在当前 worker, 交给 BusServer 判断并 pipe message 到正确的 worker
            $this->cmdBusServer(Constants::CLIENT_EVENT_EMIT, swoole_serialize::pack([
                'clients' => $pollingSessions,
                'data' => $message,
                'binaries' => $binaries
            ]));
        }
        if (count($webSocketSessions)) {
            $this->sendMessageToClient($webSocketSessions, $message, $binaries, false);
        }
        return $this;
    }

    /**
     * 发消息给连接在当前节点的指定客户端(们) sessionIds 在当前节点且在当前 worker
     * @param $sessionIds
     * @param $message
     * @param array $binaries
     * @param bool $polling
     * @return BusContext
     */
    public function sendMessageToClient($sessionIds, $message, array $binaries = [], $polling = false)
    {
        if (!is_array($sessionIds)) {
            $sessionIds = [$sessionIds];
        }
        foreach ($sessionIds as $sessionId) {
            if ($polling) {
                $this->sendMessageToPolling($sessionId, $message, $binaries);
            } else {
                $this->sendMessageToWebSocket($sessionId, $message, $binaries);
            }
        }
        return $this;
    }

    /**
     * 发送消息给 连接在当前节点的指定 webSocket 客户端
     * @param int|Socket $sessionId 客户端连接
     * @param string $message 消息文本内容
     * @param array $binaries 消息二进制内容
     * @return $this
     */
    public function sendMessageToWebSocket($sessionId, $message, array $binaries = [])
    {
        try {
            Buffer::sendMessageToClient($this->swooleServer, $sessionId, $message, $binaries);
        } catch (\Throwable $e) {
            Event::resolveException($e);
        } catch (\Exception $e) {
            Event::resolveException($e);
        }
        return $this;
    }

    /**
     * 短连接发消息给 连接在当前节点的指定 eventSource 客户端
     * @param $sessionId
     * @param $message
     * @param array $binaries
     * @return $this
     */
    public function sendMessageToPolling($sessionId, $message, array $binaries = [])
    {
        $this->sendToEventSource($sessionId, $message);
        foreach ($binaries as $binary) {
            if (!($binary instanceof Binary)) {
                continue;
            }
            $this->sendToEventSource($sessionId, base64_encode($binary->buffer()), 'binary');
        }
        return $this;
    }
}
