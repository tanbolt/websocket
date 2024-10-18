<?php
namespace Tanbolt\Websocket\Workerman;

use Workerman\Connection\TcpConnection;

class WsProtocol
{
    /**
     * WebSocket blob type
     */
    const BINARY_TYPE_TEXT = "\x81";

    /**
     * WebSocket arrayBuffer type.
     */
    const BINARY_TYPE_BINARY= "\x82";

    /**
     * @var int
     */
    public static $maxPackageSize = 10485760;

    /**
     * The supported HTTP methods
     * @var array
     */
    public static $methods = array('GET', 'POST');

    /**
     * 接收数据
     * @param string $buffer
     * @param TcpConnection $connection
     * @return int
     */
    public static function input($buffer, $connection)
    {
        if (empty($connection->handshakeType)) {
            // handshake
            return static::resolveHandshake($buffer, $connection);
        } elseif ($connection->handshakeType > 1) {
            // webSocket
            return static::webSocketInput($buffer, $connection);
        } else {
            // http
            return static::httpInput($buffer, $connection);
        }
    }

    /**
     * 处理握手, 可握手普通 http 或 webSocket
     * @param $buffer
     * @param TcpConnection $connection
     * @return int
     */
    protected static function resolveHandshake($buffer, $connection)
    {
        $handshakeType = static::getHandshakeType($buffer, $connection);
        if ($handshakeType > 1) {
            // http
            $connection->handshakeType = 1;
            return static::HttpHandshake($buffer, $connection);
        } elseif ($handshakeType > 0) {
            // websocket
            $connection->handshakeType = 2;
            return static::webSocketHandshake($buffer, $connection);
        }
        return 0;
    }

    /**
     * 获取握手协议 http 还是 webSocket
     * @param $buffer
     * @param TcpConnection $connection
     * @return int
     */
    protected static function getHandshakeType($buffer, $connection)
    {
        if (strlen($buffer) < 7) {
            return 0;
        }
        // Request method
        $method = substr($buffer, 0, 7);
        $method = strpos($method, ' ') ? explode(' ', $method)[0] : $method;
        if (!in_array($method, static::$methods)) {
            return static::badHandshake($connection, 405);
        }
        // wait header
        $pos = strpos($buffer, "\r\n\r\n");
        if (!$pos) {
            if (strlen($buffer) >= static::$maxPackageSize) {
                $connection->close();
            }
            return 0;
        }
        $requestHeader = explode("\r\n", substr($buffer, 0, $pos));
        // server
        $server = array(
            'QUERY_STRING'         => '',
            'REQUEST_METHOD'       => '',
            'REQUEST_URI'          => '',
            'PATH_INFO'            => '/',
            'SERVER_PROTOCOL'      => '',
            'SERVER_NAME'          => '',
            'HTTP_HOST'            => '',
            'HTTP_USER_AGENT'      => '',
            'HTTP_ACCEPT'          => '',
            'HTTP_ACCEPT_LANGUAGE' => '',
            'HTTP_ACCEPT_ENCODING' => '',
            'HTTP_COOKIE'          => '',
            'HTTP_CONNECTION'      => '',
            'CONTENT_TYPE'         => '',
            'REMOTE_ADDR'          => '',
            'REMOTE_PORT'          => '0',
            'REQUEST_TIME'         => time()
        );
        list(
            $server['REQUEST_METHOD'],
            $server['REQUEST_URI'],
            $server['SERVER_PROTOCOL']
        ) = explode(' ', array_shift($requestHeader));

        // server & header
        $header = $cookie = [];
        foreach ($requestHeader as $line) {
            if (empty($line) || !strpos($line, ':')) {
                continue;
            }
            list($key, $value) = explode(':', $line, 2);
            $value = trim($value);
            if (empty($value)) {
                continue;
            }
            $key = str_replace('-', '_', strtoupper($key));
            if ($key === 'HOST') {
                $tmp = explode(':', $value);
                $server['SERVER_NAME'] = $tmp[0];
                if (isset($tmp[1])) {
                    $server['SERVER_PORT'] = $tmp[1];
                }
            } elseif ($key === 'COOKIE') {
                parse_str(str_replace('; ', '&', $value), $cookie);
            } elseif ($key === 'CONTENT_TYPE') {
                if (!preg_match('/boundary="?(\S+)"?/', $value, $match)) {
                    if ($pos = strpos($value, ';')) {
                        $server['CONTENT_TYPE'] = substr($value, 0, $pos);
                    } else {
                        $server['CONTENT_TYPE'] = $value;
                    }
                } else {
                    $server['CONTENT_TYPE'] = 'multipart/form-data';
                }
            } elseif ($key === 'CONTENT_LENGTH') {
                $server['CONTENT_LENGTH'] = $value;
            }
            $server['HTTP_' . $key] = $value;
            $header[join('-', array_map('ucfirst', explode('_', strtolower($key))))] = $value;
        }
        $server['REMOTE_ADDR'] = $connection->getRemoteIp();
        $server['REMOTE_PORT'] = $connection->getRemotePort();

        // query
        $query = [];
        $urls = parse_url($server['REQUEST_URI']);
        if (isset($urls['path'])) {
            $server['PATH_INFO'] = $urls['path'];
        }
        if (isset($urls['query'])) {
            $server['QUERY_STRING'] = $urls['query'];
            parse_str($urls['query'], $query);
        }

        // connection http request
        $connection->httpRequest = compact('query', 'header', 'server', 'cookie');
        if (isset($header['Connection']) && stripos($header['Connection'], 'Upgrade') !== false &&
            isset($header['Upgrade']) && strtolower($header['Upgrade']) === 'websocket' &&
            isset($header['Sec-Websocket-Key']) && isset($header['Sec-Websocket-Version'])
        ) {
            return $method === 'GET' ? 1 : static::badHandshake($connection, 405);
        }
        return $method === 'POST' ? 2 : static::badHandshake($connection, 405);
    }

    /**
     * 错误的 TCP 连接
     * @param TcpConnection $connection
     * @param int $code
     * @return int
     */
    protected static function badHandshake($connection, $code = 400)
    {
        if ($code === 405) {
            $status = 'Method Not Allowed';
        } elseif ($code === 403) {
            $status = 'Forbidden';
        } else {
            $code = 400;
            $status = 'Bad Request';
        }
        $connection->send("HTTP/1.1 $code $status\r\n\r\n", true);
        $connection->close();
        return 0;
    }

    /**
     * webSocket 握手
     * @param $buffer
     * @param TcpConnection $connection
     * @return int
     */
    protected static function webSocketHandshake($buffer, $connection)
    {
        if (empty($connection->httpRequest)) {
            return 0;
        }
        $WebSocketKey = $connection->httpRequest['header']['Sec-Websocket-Key'];
        $webSocketVersion = (int) $connection->httpRequest['header']['Sec-Websocket-Version'];
        if ($webSocketVersion !== 13) {
            // 不支持的 webSocket 版本
            $connection->send("HTTP/1.1 426 Upgrade Required\r\nSec-WebSocketVersion: 13\r\n\r\n", true);
            return 0;
        }
        $headerLen = strpos($buffer, "\r\n\r\n") + 4;

        // Handshake response data.
        $new_key = base64_encode(sha1($WebSocketKey . "258EAFA5-E914-47DA-95CA-C5AB0DC85B11", true));
        $handshake_message = "HTTP/1.1 101 Switching Protocols\r\n";
        $handshake_message .= "Upgrade: websocket\r\n";
        $handshake_message .= "Sec-WebSocket-Version: 13\r\n";
        $handshake_message .= "Connection: Upgrade\r\n";
        $handshake_message .= "Sec-WebSocket-Accept: " . $new_key . "\r\n";
        $handshake_message .= "\r\n";

        /** @var TcpConnection $connection */
        // Websocket data buffer.
        $connection->websocketDataBuffer = '';
        // Current websocket frame length.
        $connection->websocketCurrentFrameLength = 0;
        // Current websocket frame data.
        $connection->websocketCurrentFrameBuffer = '';
        // Consume handshake data.
        $connection->consumeRecvBuffer($headerLen);
        // Send handshake response.
        $connection->send($handshake_message, true);
        // Try to emit onWebSocketConnect callback.
        if (isset($connection->worker->onOpen)) {
            call_user_func($connection->worker->onOpen, $connection);
            unset($connection->onOpen, $connection->httpRequest);
        }
        // There are data waiting to be sent.
        if (!empty($connection->tmpWebsocketData)) {
            $connection->send($connection->tmpWebsocketData, true);
            $connection->tmpWebsocketData = '';
        }
        if (strlen($buffer) > $headerLen) {
            return static::input(substr($buffer, $headerLen), $connection);
        }
        return 0;
    }

    /**
     * http 握手
     * @param $buffer
     * @param TcpConnection $connection
     * @return int
     */
    protected static function HttpHandshake($buffer, $connection)
    {
        if (empty($connection->httpRequest)) {
            return 0;
        }
        $method = $connection->httpRequest['server']['REQUEST_METHOD'];
        $headerLen = strpos($buffer, "\r\n\r\n") + 4;
        if($method === 'GET' || $method === 'OPTIONS' || $method === 'HEAD') {
            return $headerLen;
        }
        if (isset($connection->httpRequest['header']['Content-Length'])) {
            return $headerLen + $connection->httpRequest['header']['Content-Length'];
        }
        return 0;
    }

    /**
     * 接收 webSocket 数据流
     * @param $buffer
     * @param TcpConnection $connection
     * @return int
     */
    protected static function webSocketInput($buffer, $connection)
    {
        $bufferLen = strlen($buffer);
        /** @var TcpConnection $connection */
        if (empty($connection->websocketCurrentFrameLength)) {
            // 数据帧首帧
            $firstByte = ord($buffer[0]);
            $secondByte = ord($buffer[1]);
            $masked = $secondByte >> 7;
            if (!$masked) {
                static::closeWebSocket($connection, 1002, 'Protocol Error');
                return 0;
            }
            $datLength = $secondByte & 127;
            $isFinFrame = $firstByte >> 7;
            $opCode = $firstByte & 0xf;
            switch ($opCode) {
                case 0x0:
                    // 中间数据包
                    break;
                case 0x1:
                    // Text 数据包
                    $connection->webSocketBinary = false;
                    break;
                case 0x2:
                    // Binary 数据包
                    $connection->webSocketBinary = true;
                    break;
                case 0x8:
                    //  close 帧
                    $code = null;
                    $reason = '';
                    if ($datLength) {
                        $reason = static::webSocketDecodeBuffer($buffer);
                        $code = unpack('n', substr($reason, 0 ,2));
                        $code = $code ? $code[1] : null;
                        if ($code) {
                            $reason = substr($reason, 2);
                            static::closeWebSocket($connection, $code, $reason, false);
                        }
                    }
                    if ($code === null) {
                        $code = 1000;
                        $reason = 'Normal Closure';
                    }
                    $connection->close(static::makeCloseFrame($code, $reason), true);
                    return 0;
                case 0x9:
                case 0xa:
                    // ping | pong, 回应 ping, pong 并不要求回应
                    if ($opCode === 0x9) {
                        $connection->send(pack('H*', '8a00'), true);
                    }
                    // Consume data from receive buffer.
                    if (!$datLength) {
                        $head_len = 6;
                        $connection->consumeRecvBuffer($head_len);
                        if ($bufferLen > $head_len) {
                            return static::input(substr($buffer, $head_len), $connection);
                        }
                        return 0;
                    }
                    // 非标准协议, ping/pong 事件对外提供, 直接发送 2
                    // 这个触发的可能性极小, 测试发现 IE10 会发送真实 pong
                    // 但因为模拟 ping/pong 的存在, 让连接始终处于活跃状态, IE10 也不发送 pong 了
                    $connection->send('2');
                    break;
                default :
                    // close: Invalid frame payload data
                    static::closeWebSocket($connection, 1007, 'Invalid frame payload data');
                    return 0;
            }
            // Calculate packet length.
            $head_len = 6;
            if ($datLength === 126) {
                $head_len = 8;
                if ($head_len > $bufferLen) {
                    return 0;
                }
                $pack = unpack('nn/ntotal_len', $buffer);
                $datLength = $pack['total_len'];
            } else {
                if ($datLength === 127) {
                    $head_len = 14;
                    if ($head_len > $bufferLen) {
                        return 0;
                    }
                    $arr = unpack('n/N2c', $buffer);
                    $datLength = $arr['c1']*4294967296 + $arr['c2'];
                }
            }
            $current_frame_length = $head_len + $datLength;
            $total_package_size = $current_frame_length;
            if (!empty($connection->websocketDataBuffer)) {
                $total_package_size += strlen($connection->websocketDataBuffer);
            }
            if ($total_package_size > static::$maxPackageSize) {
                //close: Message too big
                static::closeWebSocket($connection, 1009, 'Message too big');
                return 0;
            }
            if ($isFinFrame) {
                return $current_frame_length;
            }
            $connection->websocketCurrentFrameLength = $current_frame_length;
        } elseif ($connection->websocketCurrentFrameLength > $bufferLen) {
            // We need more frame data.
            // Return 0, because it is not clear the full packet length, waiting for the frame of fin=1.
            return 0;
        }

        // Received just a frame length data.
        if ($connection->websocketCurrentFrameLength === $bufferLen) {
            static::decode($buffer, $connection);
            $connection->consumeRecvBuffer($connection->websocketCurrentFrameLength);
            $connection->websocketCurrentFrameLength = 0;
            return 0;
        }

        // The length of the received data is greater than the length of a frame.
        if ($connection->websocketCurrentFrameLength < $bufferLen) {
            static::decode(substr($buffer, 0, $connection->websocketCurrentFrameLength), $connection);
            $connection->consumeRecvBuffer($connection->websocketCurrentFrameLength);
            $current_frame_length = $connection->websocketCurrentFrameLength;
            $connection->websocketCurrentFrameLength = 0;
            // Continue to read next frame.
            return static::input(substr($buffer, $current_frame_length), $connection);
        }
        // The length of the received data is less than the length of a frame.
        return 0;
    }

    /**
     * 关闭 webSocket
     * @param TcpConnection $connection
     * @param int $code
     * @param string $reason
     * @param bool $send
     */
    public static function closeWebSocket($connection, $code = null, $reason = '', $send = true)
    {
        $connection->closeCode = $code;
        $connection->closeReason = $reason;
        if ($send) {
            $connection->close(static::makeCloseFrame($code, $reason), true);
        }
    }

    /**
     * 获取 webSocket close frame
     * @param null $code
     * @param string $reason
     * @return string
     */
    protected static function makeCloseFrame($code = null, $reason = '')
    {
        $code = $code === null ? null : (int) $code;
        if ($code !== null &&
            !in_array($code, [1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011]) &&
            ($code < 3000 || $code > 4999)
        ) {
            $code = null;
        }
        if ($code === null) {
            return chr(136) . chr(0);
        }
        $reason = substr($reason, 0 ,123);
        return chr(136) . chr(2 + strlen($reason)) . pack('n', $code) . $reason;
    }

    /**
     * 接收 Http 数据流
     * @param $buffer
     * @param TcpConnection $connection
     * @return int
     */
    protected static function httpInput($buffer, $connection)
    {
        return 0;
    }















    /**
     * 解码接收到的数据流
     * @param string $buffer
     * @param TcpConnection $connection
     * @return string
     */
    public static function decode($buffer, $connection)
    {
        if (empty($connection->handshakeType)) {
            // handshake
            return $buffer;
        } elseif ($connection->handshakeType > 1) {
            // webSocket
            return static::webSocketDecode($buffer, $connection);
        } else {
            // http
            return static::httpDecode($buffer, $connection);
        }
    }

    /**
     * 解码接收到的 webSocket 数据流
     * @param $buffer
     * @param TcpConnection $connection
     * @return string
     */
    protected static function webSocketDecode($buffer, $connection)
    {
        $decoded = static::webSocketDecodeBuffer($buffer);
        if (empty($connection->websocketCurrentFrameLength)) {
            if (!empty($connection->websocketDataBuffer)) {
                $decoded = $connection->websocketDataBuffer . $decoded;
                $connection->websocketDataBuffer = '';
            }
            return $decoded;
        }
        if (empty($connection->websocketDataBuffer)) {
            $connection->websocketDataBuffer = $decoded;
        } else {
            $connection->websocketDataBuffer .= $decoded;
        }
        return $connection->websocketDataBuffer;
    }

    /**
     * @param $buffer
     * @return string
     */
    protected static function webSocketDecodeBuffer($buffer)
    {
        $decoded = '';
        $len = ord($buffer[1]) & 127;
        if ($len === 126) {
            $masks = substr($buffer, 4, 4);
            $data  = substr($buffer, 8);
        } elseif ($len === 127) {
            $masks = substr($buffer, 10, 4);
            $data  = substr($buffer, 14);
        } else {
            $masks = substr($buffer, 2, 4);
            $data  = substr($buffer, 6);
        }
        for ($index = 0; $index < strlen($data); $index++) {
            $decoded .= $data[$index] ^ $masks[$index % 4];
        }
        return $decoded;
    }

    /**
     * 解码接收到的 http 数据流
     * @param $buffer
     * @param TcpConnection $connection
     * @return string
     */
    protected static function httpDecode($buffer, $connection)
    {
        return '';
    }














    /**
     * 编码需要发送的数据流
     * @param mixed $buffer
     * @param TcpConnection $connection
     * @return string
     */
    public static function encode($buffer, $connection)
    {
        if (empty($connection->handshakeType)) {
            // handshake
            if (empty($connection->tmpWebsocketData)) {
                $connection->tmpWebsocketData = '';
            } else {
                $connection->tmpWebsocketData .= $buffer;
            }
            return '';
        } elseif ($connection->handshakeType > 1) {
            // webSocket
            return static::webSocketEncode($buffer, $connection);
        } else {
            // http
            return static::httpEncode($buffer, $connection);
        }
    }

    /**
     * webSocket 数据字节头
     * @param $len
     * @param bool $binary
     * @return string
     */
    public static function encodeWebSocketHeader($len, $binary = false)
    {
        $code = $binary ? static::BINARY_TYPE_BINARY : static::BINARY_TYPE_TEXT;
        if ($len <= 125) {
            return  $code . chr($len);
        } elseif ($len <= 65535) {
            return $code . chr(126) . pack("n", $len);
        }
        return $code . chr(127) . pack("NN", 0, $len);
    }

    /**
     * 编码需要发送的 webSocket 数据流
     * @param $buffer
     * @param TcpConnection $connection
     * @return string
     * @throws \Exception
     */
    protected static function webSocketEncode($buffer, $connection)
    {
        if (!empty($connection->tmpWebsocketData)) {
            $buffer = $connection->tmpWebsocketData . $buffer;
        }
        return static::encodeWebSocketHeader(strlen($buffer)) . $buffer;
    }

    /**
     * 编码需要发送的 http 数据流
     * @param $buffer
     * @param TcpConnection $connection
     * @return string
     */
    protected static function httpEncode($buffer, $connection)
    {
        return '';
    }
}
