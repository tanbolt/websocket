<?php
namespace Tanbolt\Websocket;
use Tanbolt\Websocket\Swoole\Constants;

/**
 * Class Socket
 * @package Tanbolt\Websocket
 * Server 和 Client 通信操作类
 *
 * # 客户端连接的相关的信息 (客户端不一定是连接在当前处理请求的节点上)
 * @property string $clientId    // 客户端全局唯一ID
 * @property int $clientArea     // todo 异地多活, 客户端连接到集群的区域ID  (暂不支持,恒为0)
 * @property string $clientNode // 客户端连接到节点的内网地址
 * @property string $clientNodeId // 客户端连接到节点的内网编号
 * @property string|int $clientFd  // 客户端在连接节点的唯一ID
 *
 *
 * # 处理当前请求的相关信息
 * @property int $areaId    // todo 异地多活, 当前处理请求集群的区域ID (暂不支持,恒为0)
 * @property int $nodeId    // 当前处理请求的节点 在内网集群的节点ID
 * @property string $nodeAddress // 当前处理请求的节点 在内网集群中的内网地址
 * @property int $workerId // 当前处理请求的 进程的ID
 *
 *
 * # 握手信息
 * @property bool $polling // 是否 polling 请求
 * @property bool $needReply   // 本次通信是否需要回复
 * @property int $msgType   // 本次通信类型
 * @property array $messages // 本次通信内容
 * @property array $groups // 所有组
 * @property array $attributes 所有属性
 * @property bool $offline // 是否已下线
 *
 * @property array $query // 握手 query
 * @property array $header // 握手 header
 * @property array $cookie // 握手 cookie
 * @property array $server // 握手 server
 *
 * @property bool $https // 是否 https
 * @property bool $crossDomain // 是否跨域请求
 * @property string $path // 访问 path
 * @property string $ip // 客户端 Ip
 * @property int $conTime // 握手开始时间
 * @property int $issued // 握手后到当前的时长 (单位:秒)
 */
class Socket
{
    // 消息类型
    const CLIENT_NOT_FOUND = 0; // 未能从独立进程中取得该链接用户信息
    const UNDEFINED = 1;
    const OPENED = 2;
    const MESSAGE = 4;
    const REPLY = 5;
    const CLOSED = 8;
    const ERROR = 9;

    /**
     * 客户端连接唯一ID (若为分布式, 所有节点唯一)
     * @var int
     */
    protected $uniqueId;

    /**
     * 连接用户的上下文对象
     * @var ContextInterface
     */
    protected $context;

    /**
     * 从上下文对象中获取到的 用户信息
     * [
     *      areaId, nodeId, nodeAddress, workerId,
     *      clientArea, clientNodeId, clientFd,
     *      path, connectTime, client, groups, attribute, polling
     * ]
     * client = [
     *      header => [],
     *      cookie => [],
     *      query => [],
     *      server => []
     * ]
     * groups => [name, name, ...]
     * attributes => [
     *      key => val,
     *      ....
     * ]
     * @var array
     */
    protected $client = [];

    /**
     * 用户当前的 group
     * @var array
     */
    protected $currentGroup = [];

    /**
     * 用户当前的 attr
     * @var array
     */
    protected $currentAttr = [];

    /**
     * 当前消息类型
     * @var int
     */
    protected $messageType = null;

    /**
     * 当前消息头 code
     * @var int
     */
    protected $messageCode = false;

    /**
     * 当前消息内容
     * @var array
     */
    protected $messageData = [];

    /**
     * 是否获取 client info 失败
     * @var bool
     */
    protected $error = false;

    /**
     * 当前 socket 是否已关闭
     * @var bool
     */
    protected $closed = false;

    /**
     * @var array
     */
    private $magicVar = [
        'clientId', 'clientArea', 'clientNode', 'clientNodeId', 'clientFd',
        'areaId', 'nodeId', 'nodeAddress', 'workerId',
        'polling', 'msgType', 'messages', 'groups', 'attributes', 'offline',
        'query', 'header', 'cookie', 'server',
        'https', 'crossDomain', 'path', 'ip', 'conTime', 'issued',
    ];

    /**
     * 需要比较, 比较之后缓存一下结果
     * @var bool
     */
    private $ifCrossDomain = null;

    /**
     * 通过 文件路径 或 已打开文件指针 创建二进制消息
     * @param $file
     * @return Binary
     */
    public static function binary($file)
    {
        return new Binary($file);
    }

    /**
     * 通过 二进制字符串 创建 二进制消息
     * @param $data
     * @return Binary
     */
    public static function binaryStr($data)
    {
        return new Binary($data, true);
    }

    /**
     * Socket constructor.
     * @param string $uniqueId 即 clientId 是由 areaId, nodeId, sessionId 组合的唯一值
     * @param ContextInterface $Context
     * @param array $messages
     */
    public function __construct($uniqueId, ContextInterface $Context, array $messages = [])
    {
        $this->uniqueId = $uniqueId;
        $this->context = $Context;
        $this->client = $this->context->getClient($uniqueId);
        if (is_array($this->client) && count($this->client) === 13) {
            $this->currentGroup = $this->client[10];
            $this->currentAttr = $this->client[11];
            $this->initMessages($messages);
        } else {
            $this->error = true;
            $this->messageType = static::CLIENT_NOT_FOUND;
        }
    }

    /**
     * 初始化消息 传入消息格式为 [code, [event, ...arg] ]
     * @param array $messages
     * @return $this
     */
    protected function initMessages(array $messages = [])
    {
        if (!count($messages)) {
            $this->messageType = static::UNDEFINED;
            return $this;
        }
        $this->messageType = static::ERROR;
        $code = (int) array_shift($messages);
        if ($code === 0) {
            $this->messageType = static::OPENED;
        } elseif ($code === 8) {
            $this->closed = true;
            $this->messageType = static::CLOSED;
        } elseif ($len = strlen($code)) {
            $this->messageCode = $len > 1 ? (int) substr($code, 1) : false;
            $code = (int) substr($code, 0, 1);
            if ($code === 3) {
                $this->messageType = static::MESSAGE;
            } elseif ($code === 4) {
                $this->messageType = $this->messageCode === false ? static::ERROR : static::REPLY;
            }
        }
        return $this->setMessages($messages);
    }

    /**
     * 重设本次通信收到的消息 (不含 code 值)
     * @param array $messages
     * @return $this
     */
    public function setMessages($messages = [])
    {
        if ($messages === 'closed') {
            $this->closed = true;
        } elseif (is_array($messages)) {
            $this->messageData = $messages;
        }
        return $this;
    }

    /**
     * 检测是否已经关闭
     * @param string $msg
     * @return $this
     */
    protected function checkIfErrorOrClosed($msg = '')
    {
        if ($this->error) {
            throw new \RuntimeException((empty($msg) ? '' : $msg.' ').'Client not found');
        }
        if ($this->closed) {
            throw new \RuntimeException((empty($msg) ? '' : $msg.' ').'Client ['.$this->clientId.'] was closed');
        }
        return $this;
    }

    /**
     * flatt array
     * @param $array
     * @return array
     */
    protected function flatten($array)
    {
        $return = [];
        array_walk_recursive($array, function ($x) use (&$return) {
            if ($x) {
                $return[] = $x;
            }
        });
        return array_unique($return);
    }

    /**
     * 给用户添加新的组  join(a, b, [c, d])
     * @param string|array $group
     * @return $this
     */
    public function join($group)
    {
        $this->checkIfErrorOrClosed('Join group failed');
        $this->currentGroup = array_keys(array_flip(array_merge($this->currentGroup, $this->flatten(func_get_args()))));
        return $this;
    }

    /**
     * 移除用户所属组 leave(a, b, [c, d])
     * @param string|array $group
     * @return $this
     */
    public function leave($group)
    {
        $this->checkIfErrorOrClosed('Leave group failed');
        $this->currentGroup = array_values(array_diff($this->currentGroup, $this->flatten(func_get_args())));
        return $this;
    }

    /**
     * 设置用户所属组 (若 group==null 用户将成为独立用户,不属于任何组)
     * @param string|array|null $group
     * @return $this
     */
    public function setGroup($group = null)
    {
        $this->checkIfErrorOrClosed('Set group failed');
        $this->currentGroup = $group ? $this->flatten(func_get_args()) : [];
        return $this;
    }

    /**
     * 给当前 socket 设置属性, 可设置 key 为数组同时设置多个
     * @param string|array $key
     * @param null $val
     * @return $this
     */
    public function setAttr($key, $val = null)
    {
        $this->checkIfErrorOrClosed('Set attribute failed');
        if (is_array($key)) {
            $this->currentAttr = array_merge($this->currentAttr, $key);
        } else {
            $this->currentAttr[$key] = $val;
        }
        return $this;
    }

    /**
     * 移除当前 socket 的指定属性
     * @param $key
     * @return $this
     */
    public function removeAttr($key)
    {
        $this->checkIfErrorOrClosed('Remove attribute failed');
        $key = $this->flatten(func_get_args());
        foreach ($key as $k) {
            if (isset($this->currentAttr[$k])) {
                unset($this->currentAttr[$k]);
            }
        }
        return $this;
    }

    /**
     * 清空当前 socket 属性
     * @return $this
     */
    public function clearAttr()
    {
        $this->checkIfErrorOrClosed('Clear attribute failed');
        $this->currentAttr = [];
        return $this;
    }

    /**
     * 获取当前 socket 属性, key 为 null 则获取所有
     * @param string|null $key
     * @return array|null
     */
    public function getAttr($key = null)
    {
        if (empty($key)) {
            return $this->currentAttr;
        }
        return $this->currentAttr[$key] ?? null;
    }

    /**
     * 获取指定 path 下所有客户端 ID
     * 指定的路径, 可通过设置 $path 为数组同时获取多个 path 下的用户, 若不指定则获取当前用户所属 path 的所有客户端
     * @param string|array $path
     * @return array
     */
    public function pathClients($path = null)
    {
        return $this->context->getPathClient($path === null ? $this->path : $this->flatten(func_get_args()));
    }

    /**
     * 获取指定 group 下所有客户端 ID
     * 指定的路径, 可通过设置 $group 为数组同时获取多个 group 下的用户, 若不指定则获取当前用户所属 group 的所有客户端
     * @param string|array $group
     * @return array
     */
    public function groupClients($group = null)
    {
        return $this->context->getGroupClient($group === null ? $this->groups : $this->flatten(func_get_args()));
    }

    /**
     * 回复本次通信
     * @param null $args
     * @return $this
     */
    public function reply($args = null)
    {
        if ($this->messageCode === false) {
            return $this;
        }
        $this->checkIfErrorOrClosed('Reply failed');
        $this->context->emit($this->uniqueId, func_get_args(), $this->messageCode);
        return $this;
    }

    /**
     * 发消息给自己  emit(event, [...args, ack])
     * args 可以是字符串或二进制数据流, php 判断字符串是否为二进制很难获得绝对正确的结果
     * 所以要发送 二进制数据, 需要显性指明
     *      $socket->emit('event', $socket::binary(file));
     * 也可以混搭使用 如
     *      $socket->emit('event', 1, '2', $socket::binary(file))
     * 使用 ack
     *      $socket->emit('event', 1, '2', $socket::binary(file), function() {
     *      })
     * @param string $event
     * @param null $args
     * @param callable $ack
     * @return $this
     */
    public function emit($event, $args = null, $ack = null)
    {
        $this->checkIfErrorOrClosed('Emit failed');
        $this->context->emit($this->uniqueId, func_get_args(), true);
        return $this;
    }

    /**
     * 发送消息给指定的 clientIds, 可通过 array 做为参数同时发送给多个用户
     * @param string|array $clientIds
     * @param string $event
     * @param null $args
     * @return $this
     */
    public function emitTo($clientIds, $event, $args = null)
    {
        $args = func_get_args();
        $clientIds = array_shift($args);
        $this->context->broadcast(Constants::FORWARD_CLIENT, $clientIds, $args);
        return $this;
    }

    /**
     * 发送消息给当前用户所在 path 下的其他用户 (当前用户除外)
     * @param string $event
     * @param null $args
     * @return $this
     */
    public function emitPath($event, $args = null)
    {
        $this->context->broadcast(Constants::FORWARD_PATH, $this->path, func_get_args(), $this->clientFd);
        return $this;
    }

    /**
     * 发送消息给指定 path 下的所有用户 (包括当前用户)
     * @param string $event
     * @param null $args
     * @return $this
     */
    public function castPath($event, $args = null)
    {
        $this->context->broadcast(Constants::FORWARD_PATH, $this->path, func_get_args());
        return $this;
    }

    /**
     * 发送消息给指定 path 下的其他用户 (当前用户除外)
     * @param string|array $path  可通过数组同时指定多个
     * @param string $event
     * @param null $args
     * @return $this
     */
    public function emitToPath($path, $event, $args = null)
    {
        $args = func_get_args();
        $path = array_shift($args);
        $this->context->broadcast(Constants::FORWARD_PATH, $path, $args, $this->clientFd);
        return $this;
    }

    /**
     * 发送消息给指定 path 下的所有用户 (包括当前用户)
     * @param string|array $path  可通过数组同时指定多个
     * @param string $event
     * @param null $args
     * @return $this
     */
    public function castToPath($path, $event, $args = null)
    {
        $args = func_get_args();
        $path = array_shift($args);
        $this->context->broadcast(Constants::FORWARD_PATH, $path, $args);
        return $this;
    }

    /**
     * 发送消息给当前用户所在 group 下的其他用户 (当前用户除外)
     * @param string $event
     * @param null $args
     * @return $this
     */
    public function emitGroup($event, $args = null)
    {
        $this->context->broadcast(Constants::FORWARD_GROUP, $this->groups, func_get_args(), $this->clientFd);
        return $this;
    }

    /**
     * 发送消息给指定 group 下的所有用户 (包括当前用户)
     * @param string $event
     * @param null $args
     * @return $this
     */
    public function castGroup($event, $args = null)
    {
        $this->context->broadcast(Constants::FORWARD_GROUP, $this->groups, func_get_args());
        return $this;
    }

    /**
     * 发送消息给指定 group 下的其他用户 (当前用户除外)
     * @param string|array $group  可通过数组同时指定多个
     * @param string $event
     * @param null $args
     * @return $this
     */
    public function emitToGroup($group, $event, $args = null)
    {
        $args = func_get_args();
        $group = array_shift($args);
        $this->context->broadcast(Constants::FORWARD_GROUP, $group, $args, $this->clientFd);
        return $this;
    }

    /**
     * 发送消息给指定 group 下的所有用户 (包括当前用户)
     * @param string|array $group  可通过数组同时指定多个
     * @param string $event
     * @param null $args
     * @return $this
     */
    public function castToGroup($group, $event, $args = null)
    {
        $args = func_get_args();
        $group = array_shift($args);
        $this->context->broadcast(Constants::FORWARD_GROUP, $group, $args);
        return $this;
    }

    /**
     * 发消息给所有在线的其他人 (当前用户除外)
     * @param string $event
     * @param null $args
     * @return $this
     */
    public function emitWorld($event, $args = null)
    {
        $this->context->broadcast(Constants::FORWARD_WORLD, null, func_get_args(), $this->clientFd);
        return $this;
    }

    /**
     * 发消息给所有在线的人 (包括当前用户)
     * @param string $event
     * @param null $args
     * @return $this
     */
    public function castWorld($event, $args = null)
    {
        $this->context->broadcast(Constants::FORWARD_WORLD, null, func_get_args());
        return $this;
    }

    /**
     * 关闭当前连接
     * @param null $code
     * @param string $reason
     * @return $this
     */
    public function close($code = null, $reason = '')
    {
        if ($this->msgType === static::OPENED) {
            $exception = new \RuntimeException($reason);
            $exception->closeCode = $code;
            throw $exception;
        }
        if (!$this->error) {
            $this->closed = true;
            $this->kickoff($this->uniqueId, $code, $reason);
        }
        return $this;
    }

    /**
     * 下线指定的客户端
     * @param $clientId
     * @param null $code
     * @param string $reason
     * @return $this
     */
    public function kickoff($clientId, $code = null, $reason = '')
    {
        $this->context->closeClient($clientId, $code, $reason);
        return $this;
    }

    /**
     * 实例化一个其他客户端的 socket, 后续可对其设置 group attr 或发消息等
     * @param $clientId
     * @return static
     */
    public function instance($clientId)
    {
        return new static($clientId, $this->context, []);
    }

    /**
     * 更新 group attributes
     */
    public function __destruct()
    {
        if (!$this->error && !$this->closed) {
            $addGroup = array_values(array_diff($this->currentGroup, $this->client[10]));
            $subGroup = array_values(array_diff($this->client[10], $this->currentGroup));
            $alterAttr = [];
            foreach ($this->currentAttr as $key => $value) {
                if (!isset($this->client[11][$key])) {
                    $alterAttr[$key] = $value;
                } elseif ($this->client[11][$key] !== $value) {
                    $alterAttr[$key] = $value;
                }
            }
            if (count($addGroup) || count($subGroup) || count($alterAttr)) {
                $this->context->updateClient($this->clientId, $addGroup, $subGroup, $alterAttr);
            }
        }
    }

    /**
     * @param $name
     * @return bool
     */
    public function __isset($name)
    {
        return in_array($name, $this->magicVar);
    }

    /**
     * @param $name
     * @return string|array
     */
    public function __get($name)
    {
        switch ($name) {
            case 'clientId':
                return $this->error ? null : $this->uniqueId;
            case 'clientArea':
                return $this->error ? null : $this->client[4];
            case 'clientNode':
                return $this->error ? null : $this->client[9]['server']['SERVER_ADDR'].':'.$this->client[9]['server']['SERVER_PORT'];
            case 'clientNodeId':
                return $this->error ? null : $this->client[5];
            case 'clientFd':
                return $this->error ? null : $this->client[6];
            case 'areaId':
                return $this->error ? null : $this->client[0];
            case 'nodeId':
                return $this->error ? null : $this->client[1];
            case 'nodeAddress':
                return $this->error ? null : $this->client[2];
            case 'workerId':
                return $this->error ? null : $this->client[3];
            case 'polling':
                return $this->error ? null : (bool) $this->client[12];
            // _realFd, _headerCode供 BusContext 使用, 外部最好不要使用
            case '_realFd':
                if ($this->client[12]) {
                    return is_array($this->client[12]) ? $this->client[12][0] : null;
                }
                return $this->error ? null : $this->client[6];
            case '_headerCode':
                return $this->messageCode;
            case 'msgType':
                return $this->messageType;
            case 'needReply':
                return $this->messageCode !== false;
            case 'messages':
                return $this->messageData;
            case 'groups':
                return $this->currentGroup;
            case 'attributes':
                return $this->currentAttr;
            case 'offline':
                return $this->closed;
            case 'query':
            case 'header':
            case 'cookie':
            case 'server':
                return $this->error ? [] : $this->client[9][$name];
            case 'https':
                return $this->error ? null : isset($this->client[9]['server']['HTTPS']);
            case 'crossDomain':
                if ($this->ifCrossDomain !== null) {
                    return $this->ifCrossDomain;
                }
                if (isset($this->header['Origin']) && isset($this->header['Host'])) {
                    $urls = parse_url($this->header['Origin']);
                    return $this->ifCrossDomain = !(
                        isset($urls['host']) &&
                        strtolower($urls['host']) === strtolower($this->header['Host'])
                    );
                }
                return $this->ifCrossDomain = false;
            case 'path':
                return $this->error ? null : $this->client[7];
            case 'ip':
                return $this->error ? null : $this->client[9]['server']['REMOTE_ADDR'];
            case 'conTime':
                return $this->error ? null : $this->client[8];
            case 'issued':
                return $this->error ? null : time() - $this->client[8];
            default:
                throw new \InvalidArgumentException('Undefined property: '.__CLASS__.'::$'.$name);
        }
    }
}
