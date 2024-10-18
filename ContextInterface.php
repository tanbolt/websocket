<?php
namespace Tanbolt\Websocket;

/**
 * Class Context
 * @package Tanbolt\Websocket\Context
 * 负责与客户端交互的上下文
 */
interface ContextInterface
{
    /**
     * 获取已进入用户信息
     * 返回 [areaId, nodeId, sessionId, workerId, path, connectTime, client, groups, attribute]
     * @param $clientId
     * @return array
     */
    public function getClient($clientId);

    /**
     * 更新用户信息
     * @param $clientId
     * @param array $addGroup
     * @param array $subGroup
     * @param array $alterAttr
     * @return mixed
     */
    public function updateClient($clientId, array $addGroup, array $subGroup, array $alterAttr);

    /**
     * 关闭指定用户连接
     * @param $clientId
     * @param null $code
     * @param string $reason
     * @return mixed
     */
    public function closeClient($clientId, $code = null, $reason = '');

    /**
     * 获取指定 path 下所有用户id
     * @param $path
     * @return array
     */
    public function getPathClient($path);

    /**
     * 获取指定 path 下所有用户id
     * @param $group
     * @return array
     */
    public function getGroupClient($group);

    /**
     * 发消息给指定的 clientId
     * @param $clientId
     * @param array $data
     * @param bool $replyCode
     * @return mixed
     */
    public function emit($clientId, array $data = [], $replyCode = false);

    /**
     * 广播消息给指定范围内的客户端
     * @param $scopeType
     * @param $scope
     * @param array $data
     * @param null $exSession
     * @return mixed
     */
    public function broadcast($scopeType, $scope, array $data = [], $exSession = null);
}
