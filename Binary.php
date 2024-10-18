<?php
namespace Tanbolt\Websocket;

/**
 * Class Binary
 * @package Tanbolt\Websocket
 * 二进制消息内容
 */
class Binary implements \Iterator
{
    /**
     * 原始参数 (文件路径 / handle / binary data)
     * @var mixed
     */
    protected $file = null;

    /**
     * 文件 handle
     * @var resource
     */
    protected $fileHandle = null;

    /**
     * @var bool
     */
    protected $isString = false;

    /**
     * 文件大小
     * @var int
     */
    protected $fileSize = null;

    /**
     * 每次读取文件 KB
     * @var int
     */
    private $chunkSize = 8192;

    /**
     * Iterator position
     * @var int
     */
    private $position = 0;

    /**
     * Binary constructor.
     * @param $file
     * @param bool $string
     */
    public function __construct($file, $string = false)
    {
        $this->isString = (bool) $string;
        if ($this->isString) {
            $this->file = $file;
            $this->fileHandle = null;
            $this->fileSize = strlen($this->file);
        } elseif (is_resource($file)) {
            $this->fileHandle = $file;
            $meta = stream_get_meta_data($file);
            $this->file = is_array($meta) && isset($meta['uri']) ? $meta['uri'] : null;
            fseek($this->fileHandle, 0, SEEK_END);
            $this->fileSize = ftell($this->fileHandle);
            fseek($this->fileHandle, 0);
        } else {
            $this->file = $file;
            $this->fileHandle = fopen($file, 'rb');
            $this->fileSize = ($fileSize = filesize($file)) < 0 ? null : $fileSize;
        }
    }

    /**
     * close file handle
     */
    public function __destruct()
    {
        set_error_handler(function(){});
        if ($this->fileHandle) {
            fclose($this->fileHandle);
        }
        restore_error_handler();
    }

    /**
     * 是否为字符串数据
     * @return bool
     */
    public function isString()
    {
        return $this->isString;
    }

    /**
     * 读取文件的指针
     * @return bool|resource
     */
    public function fileHandle()
    {
        return $this->fileHandle;
    }

    /**
     * 文件路径 (字符串数据返回 null)
     * @return string
     */
    public function filePath()
    {
        return $this->isString ? null : $this->file;
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
        if ($this->isString) {
            return $this->file;
        }
        return stream_get_contents($this->fileHandle);
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
     * 将当期 binary 保存到指定的文件
     * @param $filePath
     * @param bool $append
     * @return $this
     */
    public function saveTo($filePath, $append = false)
    {
        $fp = fopen($filePath, $append ? 'a' : 'w');
        $this->rewind();
        while ($this->valid()) {
            fwrite($fp, $this->current());
            $this->next();
        }
        return $this;
    }

    /**
     * @return $this
     */
    public function rewind()
    {
        $this->position = 0;
        if ($this->fileHandle) {
            fseek($this->fileHandle, $this->position);
        }
        return $this;
    }

    /**
     * @return int
     */
    public function key()
    {
        return $this->position;
    }

    /**
     * @return string
     */
    public function current()
    {
        if (!$this->valid()) {
            return null;
        }
        if ($this->isString) {
            return substr($this->file, $this->position, $this->chunkSize);
        }
        fseek($this->fileHandle, $this->position);
        $dat = fread($this->fileHandle, $this->chunkSize);
        flush();
        return $dat;
    }

    /**
     * @return $this
     */
    public function next()
    {
        $this->position = $this->isString ? $this->position + $this->chunkSize : ftell($this->fileHandle);
        return $this;
    }

    /**
     * @return bool
     */
    public function valid()
    {
        return $this->isString ? $this->position < $this->fileSize : $this->fileHandle && !feof($this->fileHandle);
    }
}
