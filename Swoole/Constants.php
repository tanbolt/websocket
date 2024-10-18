<?php
namespace Tanbolt\Websocket\Swoole;

class Constants
{
    // worker 与  内存独立进程 之间的通信类型
    // 其中 CLIENT_GET/CLIENT_UPDATE/CLIENT_DROP/PATH_GET/GROUP_GET/FORWARD 可能会跨节点通信
    // CLIENT_GET/CLIENT_DROP/PATH_GET/EMIT_CLIENTS 需等待节点回复
    const NODE_REGISTER = 1;
    const NODE_ADD = 2;
    const NODE_ID = 3;

    const CLIENT_ADD = 10;
    const CLIENT_GET = 11;
    const CLIENT_UPDATE = 12;
    const CLIENT_UPHEADER = 13;
    const CLIENT_DROP = 14;
    const CLIENT_CLOSE = 15;

    const CLIENT_EVENT_CONNECT = 16;
    const CLIENT_EVENT_EMIT = 17;
    const CLIENT_EVENT_CLOSE = 18;

    const CLIENT_REQUEST_OPEN = 19;
    const CLIENT_REQUEST_EMIT = 20;
    const CLIENT_REQUEST_CLOSE = 21;

    const PATH_GET = 40;
    const GROUP_GET = 41;

    const FORWARD = 50;
    const FORWARD_CLIENT = 51;
    const FORWARD_PATH = 52;
    const FORWARD_GROUP = 53;
    const FORWARD_WORLD = 54;

    const EMIT_CLIENTS = 60;
}
