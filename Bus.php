<?php
namespace Tanbolt\Websocket;

/**
 * Trait Memory
 * @package Tanbolt\Websocket\IoEvent
 * 以内存形式 存储当期服务器的连接用户的个人信息
 *
 * 若服务器故障, 其维护的客户端掉线, 自动重连后会分配到其他节点
 * 所以服务器连接用户无需落地存储, 使用内存即可
 * 因为没有统一存储, 带来一个坏处, 不能快速获取所有节点的在线用户
 * 但相比其带来的优势, 是可以接受的, 并且也可以通过获取所有节点用户后组合返回
 */
trait Bus
{
    /**
     * 用户信息
     * [
     *    sessionId =>  [path, connectTime, client, groups, attribute, polling],
     *    .....
     * ]
     *
     * sessionId = string|int, 当前节点唯一值
     *
     * path = string, 当期连接 URL 中的 path 部分
     *
     * client = [
     *      query => [],
     *      header => [],
     *      server => [],
     *      cookie => [],
     * ]
     * groups => [name, name, ...]
     * attributes => [
     *      key => val,
     *      ....
     * ]
     *
     * @var array
     */
    protected $clients = [];

    /**
     * 索引连接用户所在 path
     * [
     *      path => [sessionId, ....],
     *      ....
     * ]
     * @var array
     */
    protected $paths = [];

    /**
     * 索引用户组, 以空间换时间
     * [
     *     group => [sessionId, ....],
     *     .....
     * ]
     * @var array
     */
    protected $groups = [];

    /**
     * 用户进入
     * @param $sessionId
     * @param array $request
     * @return bool
     */
    protected function memAddClient($sessionId, array $request = [])
    {
        $client = [];
        foreach (['header', 'cookie', 'query', 'server'] as $key) {
            $client[$key] = isset($request[$key]) && is_array($request[$key]) ? $request[$key] : [];
        }
        $path = $request['server']['PATH_INFO'] ?? '/';
        $path = rtrim($path, '/') . '/';
        if (!isset($this->paths[$path])) {
            $this->paths[$path] = [];
        }
        $polling = $request['polling'] ?? false;
        $this->paths[$path][$sessionId] = (bool) $polling;
        $this->clients[$sessionId] = [$path, time(), $client, [], [], $polling];
        return true;
    }

    /**
     * 更新用户连接信息中的 header
     * @param $sessionId
     * @param array $headers
     * @return bool
     */
    protected function memUpdateHeader($sessionId, array $headers = [])
    {
        if (!isset($this->clients[$sessionId])) {
            return false;
        }
        $headers = array_combine(array_map(function($key) {
            return join('-', array_map('ucfirst', explode('-', strtolower($key))));
        }, array_keys($headers)), $headers);
        $this->clients[$sessionId][2]['header'] = array_merge($this->clients[$sessionId][2]['header'], $headers);
        return true;
    }

    /**
     * 用户离线
     * @param $sessionId
     * @return bool
     */
    protected function memDropClient($sessionId)
    {
        if (!isset($this->clients[$sessionId])) {
            return false;
        }
        $this->memRemoveClientFromPath($sessionId);
        $this->memRemoveClientFromGroup($this->clients[$sessionId][3], $sessionId);
        unset($this->clients[$sessionId]);
        return true;
    }

    /**
     * 是否存在指定的用户
     * @param $sessionId
     * @return bool
     */
    protected function memExistClient($sessionId)
    {
        return isset($this->clients[$sessionId]);
    }

    /**
     * 获取用户信息 返回 [path, connectTime, client, groups, attribute]
     * @param $sessionId
     * @return array|null
     */
    protected function memGetClient($sessionId)
    {
        if (!isset($this->clients[$sessionId])) {
            return null;
        }
        return $this->clients[$sessionId];
    }

    /**
     * 获取指定 path 下所有连接用户
     * @param string|array $path
     * @param bool $withPolling
     * @return array
     */
    protected function memGetPathClient($path, $withPolling = false)
    {
        $clients = [];
        $path = is_array($path) ? $path : [$path];
        foreach ($path as $item) {
            $item = is_string($item) && strlen($item) ? rtrim($item, '/').'/' : null;
            if ($item && isset($this->paths[$item])) {
                $clients = $clients + $this->paths[$item];
            }
        }
        return $withPolling ? $clients : array_keys($clients);
    }

    /**
     * 设置用户所属组
     * @param $sessionId
     * @param null $group
     * @return bool
     */
    protected function memSetClientGroup($sessionId, $group = null)
    {
        if (!isset($this->clients[$sessionId])) {
            return false;
        }
        if (empty($group)) {
            $this->memRemoveClientFromGroup($this->clients[$sessionId][3], $sessionId);
            $this->clients[$sessionId][3] = [];
        } else {
            $group = $group ? (is_array($group) ? $group : [$group]) : [];
            $this->memRemoveClientFromGroup(array_diff($this->clients[$sessionId][3], $group), $sessionId);
            $this->memAddClientToGroup(array_diff($group, $this->clients[$sessionId][3]), $sessionId, (bool) $this->clients[$sessionId][5]);
            $this->clients[$sessionId][3] = $group;
        }
        return true;
    }

    /**
     * 给指定用户新增所属组
     * @param $sessionId
     * @param $group
     * @return bool
     */
    protected function memAddClientGroup($sessionId, $group)
    {
        if (!isset($this->clients[$sessionId]) || empty($group)) {
            return false;
        }
        $group = is_array($group) ? $group : [$group];
        $this->clients[$sessionId][3] = array_keys(array_flip(array_merge($this->clients[$sessionId][3], $group)));
        return $this->memAddClientToGroup($group, $sessionId, (bool) $this->clients[$sessionId][5]);
    }

    /**
     * 移除指定用户的所属组
     * @param int $sessionId
     * @param string|array $group
     * @return bool
     */
    protected function memDelClientGroup($sessionId, $group)
    {
        if (!isset($this->clients[$sessionId]) || empty($group)) {
            return false;
        }
        $group = is_array($group) ? $group : [$group];
        $this->clients[$sessionId][3] = array_values(array_diff($this->clients[$sessionId][3], $group));
        return $this->memRemoveClientFromGroup($group, $sessionId);
    }

    /**
     * 获取指定用户组的所有用户ID
     * @param $group
     * @param bool $withPolling
     * @return array
     */
    protected function memGetGroupClient($group, $withPolling = false)
    {
        $clients = [];
        $group = is_array($group) ? $group : [$group];
        foreach ($group as $item) {
            if (is_string($item) && strlen($item) && isset($this->groups[$item])) {
                $clients = $clients + $this->groups[$item];
            }
        }
        return $withPolling ? $clients : array_keys($clients);
    }

    /**
     * 设置指定用户属性
     * @param $sessionId
     * @param $key
     * @param null $val
     * @return bool
     */
    protected function memSetClientAttr($sessionId, $key, $val = null)
    {
        if (!isset($this->clients[$sessionId]) || empty($key)) {
            return false;
        }
        if (is_array($key)) {
            $this->clients[$sessionId][4] = array_merge($this->clients[$sessionId][4], $key);
        } else {
            $this->clients[$sessionId][4][$key] = $val;
        }
        return true;
    }

    /**
     * 移除指定用户属性
     * @param $sessionId
     * @param $key
     * @return bool
     */
    protected function memRemoveClientAttr($sessionId, $key)
    {
        if (!isset($this->clients[$sessionId]) || empty($key)) {
            return false;
        }
        $key = is_array($key) ? $key : [$key];
        foreach ($key as $k) {
            if (isset($this->clients[$sessionId][4][$k])) {
                unset($this->clients[$sessionId][4][$k]);
            }
        }
        return true;
    }

    /**
     * 清空属性
     * @param $sessionId
     * @return bool
     */
    protected function memClearClientAttr($sessionId)
    {
        if (!isset($this->clients[$sessionId])) {
            return false;
        }
        $this->clients[$sessionId][4] = [];
        return true;
    }

    /**
     * 将指定用户添加到指定的 group 中
     * @param array $group
     * @param $sessionId
     * @param bool $polling
     * @return bool
     */
    private function memAddClientToGroup(array $group, $sessionId, $polling = false)
    {
        foreach ($group as $item) {
            if (!isset($this->groups[$item])) {
                $this->groups[$item] = [];
            }
            $this->groups[$item][$sessionId] = $polling;
        }
        return true;
    }

    /**
     * 从指定的 group 中删除指定的用户
     * @param array $group
     * @param $sessionId
     * @return bool
     */
    private function memRemoveClientFromGroup(array $group, $sessionId)
    {
        foreach ($group as $item) {
            if (isset($this->groups[$item]) && isset($this->groups[$item][$sessionId])) {
                unset($this->groups[$item][$sessionId]);
            }
        }
        return true;
    }

    /**
     * 从用户路径缓存中删除指定用户
     * @param $sessionId
     * @return bool
     */
    private function memRemoveClientFromPath($sessionId)
    {
        $path = $this->clients[$sessionId][0];
        if (isset($this->paths[$path]) && isset($this->paths[$path][$sessionId])) {
            unset($this->paths[$path][$sessionId]);
            return true;
        }
        return false;
    }
}
