<?php
namespace Tanbolt\Websocket\Swoole;

use swoole_serialize;
use swoole_websocket_server;
use Tanbolt\Websocket\Binary;
use Tanbolt\Websocket\Binaries;

/**
 * Class Buffer
 * @package Tanbolt\Websocket\Swoole
 * BusServer 收到的 BusContext 发送的消息结构
 * 可通过 reply 反向发送消息给 发来消息的 worker
 */
class Buffer
{
    /**
     * @var swoole_websocket_server
     */
    public $server;

    /**
     * 与 process 通信的 workerId
     * @var int
     */
    public $workerId;

    /**
     * worker 内部消息序号
     * @var int
     */
    public $messageNo;

    /**
     * worker 传递过来的消息类型
     * @var int
     */
    public $command;

    /**
     * worker 传递过来的消息内容
     * @var string
     */
    public $message;

    /**
     * worker 传递过来消息后, 发现需要转发给其他节点, 需重置该变量内容
     * 该变量内容会被转发给其他节点, 目前会发生转发的命令有
     * CLIENT_GET/CLIENT_UPDATE/CLIENT_DROP  EMIT
     * @var string Forward
     */
    public $forwardMessage;

    /**
     * 若转发消息除普通 txt 消息, 还有二进制消息, 需设置该变量
     * 目前仅在 EMIT 命令下可能会转发二进制消息
     * @var array
     */
    public $forwardBinaries = [];

    /**
     * 临时变量, 当命令为 EMIT, 设置转发类型 / 转发范围 / 转发消息
     * 方便 BusServer 来判断是轮询转发给其他所有节点, 还是精准转发到指定节点
     * @var int
     */
    public $emitType;
    public $emitScope;
    public $emitMessage;

    /**
     * webSocket 通过 tcp 方式每次向客户端发送最大字节数
     * 每次最大发送 2M
     * https://wiki.swoole.com/wiki/page/p-server/send.html
     * @var int
     */
    protected static $chunkSize = 2 * 1024 * 1024;

    /**
     * Buffer constructor.
     * @param array $message
     * @param swoole_websocket_server $server
     */
    public function __construct($message = [], swoole_websocket_server $server = null)
    {
        $this->server = $server;
        $this->workerId = $message['WorkerId'];
        $this->command = $message['Command'];
        $this->messageNo = $message['MessageNo'];
        $this->message = $message['Data'];
    }

    /**
     * 从 BusServer 回复消息给 worker, 必须回复, 否则 worker 会被阻塞
     * @param $message
     * @return $this
     */
    public function reply($message)
    {
        $this->server->sendMessage('0'.pack('N', $this->messageNo).$message, $this->workerId);
        return $this;
    }

    /**
     * 解包 从 worker 发送到 BusServer 的消息, 根据转发类型转发到其他节点
     * @return bool
     */
    public function unpack()
    {
        $pack = static::unpackBroadcastMessage($this->message, true);
        if (!is_array($pack)) {
            return false;
        }
        list($header, $this->emitType, $this->emitScope, $this->emitMessage, $this->forwardBinaries) = $pack;
        $this->forwardMessage = $header.$this->emitMessage;
        return true;
    }

    /**
     * 当转发类型为 FORWARD_CLIENT, worker 转发到 BusServer 的 scope 参数为
     *  $scope = [
     *      [polling => bool, area => int|string, node => int|string, session => int|string]
     *      ....
     *  ]
     * BusServer 需要转发给其他节点的 scope 参数应该为
     * $scope = [
     *      sessionId => polling
     *      ....
     * ]
     * BusServer 中重新计算 scope 后, 重新打包 Buffer 要转发的消息
     * @param $scope
     * @return bool
     */
    public function repack($scope)
    {
        $scope = swoole_serialize::pack($scope);
        $this->forwardMessage =
            pack('n', $this->emitType).
            pack('N', strlen($scope)).
            $scope.
            $this->emitMessage;
        return true;
    }

    /**
     * 跨节点发消息的逻辑为:
     *  -> 客户端发消息到节点 worker
     *  -> worker 收到消息, 根据业务逻辑决定是否群发消息
     *  -> worker 先群发送消息给连接在当前节点的客户端
     *  -> worker 将需要群发的消息发给自己节点的 BusServer
     *  -> BusServer 通过 webSocket 客户端转发给其他节点
     *  -> 其他节点 BusServer 收到消息, 根据群发范围将消息发给连接自己节点的客户端
     *
     * 该函数的工作是 打包 worker->BusServer 的消息 [type, scope, message(data & binary)]
     * @param $type
     * @param $scope
     * @param $data
     * @param array $binaries
     * @return bool|string|Binaries
     */
    public static function packBroadcastMessage($type, $scope, $data, array $binaries = [])
    {
        $header = null;
        if ($type === Constants::FORWARD_WORLD) {
            $header = '';
        } elseif (in_array($type, [Constants::FORWARD_CLIENT, Constants::FORWARD_PATH, Constants::FORWARD_GROUP])) {
            // 广播给指定客户端 $scope = [polling => bool, area => int|string, node => int|string, session => int|string]
            // 广播给指定 path 下的所有用户 $scope = (string) path
            // 广播给指定 group 下的所有用户  $scope = (array) groups
            $scope = swoole_serialize::pack($scope);
            $header = pack('N', strlen($scope)).$scope;
        }
        if ($header === null) {
            return false;
        }
        $header = pack('n', $type).$header;
        // End: 基本信息打包  [n:EmitType [,N:EmitTypeValueLen, N:EmitTypeValue] ]

        $count = count($binaries);
        // 无二进制消息, 直接返回 string 即可 [header, 0, message]
        if (!$count) {
            return $header.pack('n',0).$data;
        }
        // 有二进制消息, header 追加内容, 最终为
        // [header, n:BinariesCount, N*:EveryBinaryLength] . allBinaryBuffer
        $header .= pack('n', $count + 1);
        $length = [strlen($data)];
        foreach ($binaries as $binary) {
            if (!($binary instanceof Binary)) {
                return false;
            }
            $length[] = $binary->size();
        }
        $header .= pack('N'.count($length), ...$length);
        array_unshift($binaries, new Binary($header.$data, true));
        return new Binaries($binaries);
    }

    /**
     * 解包转发消息, 有两个地方用到
     * 1. message 从 worker 发到 BusServer, BusServer 需要知道是直接转发到指定节点, 还是群发给所有节点
     *    BusServer 确认后, 将消息通过 webSocket 客户端转发出去, 此时发送消息是需要带上 header 头的
     *    $asClient=false 解包得到 [type, scope, message, binaries] 其中 message 头部仍然附带 type&scope
     *
     * 2. 其他节点作为 webSocket 服务端, 收到了推送节点的消息, 解包群发类型和范围, 根据解包信息发送消息给符合条件的客户端
     *    $asClient=true 解包 message 的到 [type, scope, message] message 为正常要转发的消息
     *                   收到 二进制消息无需解包
     *
     * @param $message
     * @param bool $asClient
     * @return array|bool
     */
    public static function unpackBroadcastMessage($message, $asClient = false)
    {
        set_error_handler(function(){});
        $pack = static::runUnpackBroadcastMessage($message, $asClient);
        restore_error_handler();
        return $pack;
    }

    /**
     * @param $message
     * @param bool $asClient
     * @return array|bool
     */
    protected static function runUnpackBroadcastMessage($message, $asClient = false)
    {
        // emit type
        $header = substr($message, 0, 2);
        $type = unpack('n', $header);
        if (!count($type)) {
            return false;
        }
        $type = $type[1];
        $scope = null;
        $scopeHeader = '';
        if ($type === Constants::FORWARD_WORLD) {
            $message = substr($message, 2);
        } elseif (in_array($type, [Constants::FORWARD_CLIENT, Constants::FORWARD_PATH, Constants::FORWARD_GROUP])) {
            // scope 参数长度
            $lenBin = substr($message, 2, 4);
            $len = unpack('N', $lenBin);
            if (!count($len)) {
                return false;
            }
            $len = $len[1];
            // scope 二进制 binary
            $scopeBin = substr($message, 6, $len);
            $scopeHeader = $lenBin.$scopeBin;
            $scope = swoole_serialize::unpack($scopeBin);
            $message = substr($message, 6 + $len);
        } else {
            $type = null;
        }
        if (!$type) {
            return false;
        }
        // webSocket Server 收到了其他节点推送来的消息, 解密会发生在收到 text 类型消息时
        if (!$asClient) {
            return [$type, $scope, $message];
        }
        // BusServer 要转发 worker 发来的消息, 需要完整解包, 释放出 text 和 Binaries 消息
        $count = unpack('n', substr($message, 0, 2));
        if (!count($count)) {
            return false;
        }
        $count = $count[1];
        $binaries = [];
        if ($count) {
            // 有二进制消息
            $length = unpack('N*', substr($message, 2, $count * 4));
            if (count($length) !== $count) {
                return false;
            }
            $start = 2 + $count * 4;
            foreach ($length as $len) {
                $binaries[] = substr($message, $start, $len);
                $start += $len;
            }
            $message = array_shift($binaries);
        } else {
            // 仅文本消息
            $message = substr($message, 2);
        }
        return [$header.$scopeHeader, $type, $scope, $message, $binaries];
    }

    /**
     * webSocket 长连接发消息给客户端
     * @param swoole_websocket_server $server
     * @param $fd
     * @param $message
     * @param array $binaries
     */
    public static function sendMessageToClient(swoole_websocket_server $server, $fd, $message, array $binaries = [])
    {
        static::sendBinaryToClient($server, new Binary($message, true), $fd, true);
        foreach ($binaries as $binary) {
            if ($binary instanceof Binary) {
                static::sendBinaryToClient($server, $binary, $fd);
            }
        }
    }

    /**
     * 不使用 swoole websocket 的 push 函数, 直接使用 send, 方便控制流量
     * @param swoole_websocket_server $server
     * @param Binary $binary
     * @param $fd
     * @param bool $text
     */
    protected static function sendBinaryToClient(swoole_websocket_server $server, Binary $binary, $fd, $text = false)
    {
        $code = $text ? "\x81" : "\x82";
        $size = $binary->size();
        if ($size <= 125) {
            $header = $code . chr($size);
        } elseif ($size <= 65535) {
            $header = $code . chr(126) . pack("n", $size);
        } else {
            $header =  $code . chr(127) . pack("NN", 0, $size);
        }
        if ($size + strlen($header) <= static::$chunkSize) {
            $server->send($fd, $header.$binary->buffer());
        } else {
            $server->send($fd, $header);
            $key = 0;
            $binary->setChunkSize(static::$chunkSize)->rewind();
            while ($binary->valid()) {
                Swoole::debug('[Buffer].....send....->'.$key);
                $server->send($fd, $binary->current());
                $binary->next();
                $key++;
            }
        }
    }
}
