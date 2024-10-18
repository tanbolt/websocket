<?php
namespace Tanbolt\Websocket;

/**
 * Class Binaries
 * @package Tanbolt\Websocket
 * 多个二进制消息顺序拼接
 */
class Binaries implements \Iterator
{
    /**
     * 数据数组
     * @var Binary[]
     */
    protected $streams = [];

    /**
     * 总大小
     * @var int
     */
    protected $fileSize = 0;

    /**
     * 迭代器步长
     * @var int
     */
    protected $chunkSize = 8192;

    /**
     * 当期正在读取的 stream key 值
     * @var int
     */
    protected $currentKey = -1;

    /**
     * 当前正在读取的 stream
     * @var Binary
     */
    protected $currentStream = null;

    /**
     * 已完整读取 stream 的总大小
     * @var int
     */
    protected $currentPosition = 0;

    /**
     * Binaries constructor.
     * @param array $streams
     */
    public function __construct($streams = [])
    {
        $this->fileSize = 0;
        $this->streams = [];
        foreach ($streams as $stream) {
            if ($stream instanceof Binary) {
                $this->fileSize += $stream->size();
                $this->streams[] = $stream;
            }
        }
    }

    /**
     * 数据长度
     * @return int
     */
    public function size()
    {
        return $this->fileSize;
    }

    /**
     * 获取二进制字符
     * @return string
     */
    public function buffer()
    {
        $buffer = '';
        $this->setChunkSize(1024 * 1024)->rewind();
        while ($this->valid()) {
            $buffer .= $this->current();
            $this->next();
        }
        return $buffer;
    }

    /**
     * 设置迭代获取数据时, 每次获取的字节数
     * @param $chunkSize
     * @return $this
     */
    public function setChunkSize($chunkSize)
    {
        $this->chunkSize = (int) $chunkSize;
        return $this;
    }

    /**
     * 重置当前 stream
     * @return $this
     */
    protected function nextStream()
    {
        $key = $this->currentKey + 1;
        if (!isset($this->streams[$key])) {
            $this->currentStream = false;
            return $this;
        }
        if ($this->currentStream) {
            $this->currentPosition += $this->currentStream->size();
        }
        $this->currentKey = $key;
        $this->currentStream = $this->streams[$key]->setChunkSize($this->chunkSize)->rewind();
        return $this;
    }

    /**
     * @return $this
     */
    public function rewind()
    {
        $this->currentKey = -1;
        $this->nextStream();
        return $this;
    }

    /**
     * @return int
     */
    public function key()
    {
        return $this->currentPosition + $this->currentStream->key();
    }

    /**
     * @return string
     */
    public function current()
    {
        return $this->currentStream->current();
    }

    /**
     * @return $this
     */
    public function next()
    {
        $this->currentStream->next();
        return $this;
    }

    /**
     * @return bool
     */
    public function valid()
    {
        if ($this->currentStream === false) {
            return false;
        }
        if ($this->currentStream->valid()) {
            return true;
        }
        return $this->nextStream()->valid();
    }
}
