<?php
namespace Tanbolt\Websocket;

class Event
{
    /**
     * @var array
     */
    public static $listeners;

    /**
     * @var callable
     */
    public static $onNodeOffline;

    /**
     * @var callable
     */
    public static $onBoot;

    /**
     * @var callable
     */
    public static $onWorkerStart;

    /**
     * @var callable
     */
    public static $onWorkerReload;

    /**
     * @var callable
     */
    public static $onWorkerStop;

    /**
     * @var callable
     */
    public static $onOpen;

    /**
     * @var callable
     */
    public static $onMessage;

    /**
     * @var callable
     */
    public static $onClose;

    /**
     * @var callable
     */
    public static $onError;


    public static function nul()
    {
        // do nothing
    }

    /**
     * 初始化
     */
    public static function init()
    {
        static::$listeners = [];
        static::$onNodeOffline =
        static::$onBoot = static::$onWorkerStart = static::$onWorkerReload = static::$onWorkerStop =
        static::$onOpen = static::$onMessage = static::$onError = static::$onClose = static::class.'::nul';
    }

    /**
     * 注销所有绑定
     */
    public static function destroy()
    {
        static::$listeners = static::$onNodeOffline =
        static::$onBoot = static::$onWorkerStart = static::$onWorkerReload = static::$onWorkerStop =
        static::$onOpen = static::$onMessage = static::$onError = static::$onClose = null;
    }

    /**
     * 执行回调函数
     * @param callable $callback
     * @param array $arg
     * @param Socket|null $socket
     */
    public static function callListener(callable $callback, $arg = [], Socket $socket = null)
    {
        try {
            call_user_func_array($callback, $arg);
        } catch (\Exception $e) {
            static::resolveException($e, $socket);
        } catch (\Throwable $e) {
            static::resolveException($e, $socket);
        }
    }

    /**
     * 处理异常
     * @param $e
     * @param Socket|null $socket
     */
    public static function resolveException($e, Socket $socket = null)
    {
        try {
            call_user_func(Event::$onError, $e, $socket);
        } finally {
        }
    }
}
