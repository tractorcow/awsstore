<?php

namespace Madmatt\SilverStripeS3;

use Exception;
use Generator;
use InvalidArgumentException;
use League\Flysystem\Filesystem;
use League\Flysystem\Util;
use LogicException;
use ReflectionFunction;
use SilverStripe\Assets\Storage\AssetNameGenerator;
use SilverStripe\Assets\Storage\AssetStore;
use SilverStripe\Core\Config\Configurable;
use SilverStripe\Core\Injector\Injectable;
use SilverStripe\Core\Injector\Injector;
use TractorCow\AWSStore\CombinedAdapter;

class Store implements AssetStore
{
    use Injectable;
    use Configurable;

    /**
     * Filesystem for both protected and public s3 files
     *
     * @var Filesystem
     */
    private $filesystem = null;

    /**
     * Assign new flysystem backend.
     * This must implement both PublicAdapter and ProtectedAdapter
     *
     * @param Filesystem $filesystem
     * @return $this
     */
    public function setFilesystem(Filesystem $filesystem)
    {
        // Note: this adapter must provide both public and private assets
        if ((!$filesystem->getAdapter() instanceof CombinedAdapter)) {
            throw new InvalidArgumentException("Configured adapter must implement CombinedAdapter");
        }
        $this->filesystem = $filesystem;
        return $this;
    }

    /**
     * Get the currently assigned flysystem backend
     *
     * @return Filesystem
     * @throws LogicException
     */
    public function getFilesystem()
    {
        if (!$this->filesystem) {
            throw new LogicException("Filesystem misconfiguration error");
        }
        return $this->filesystem;
    }

    public function getCapabilities()
    {
        return array(
            'visibility' => array(
                self::VISIBILITY_PUBLIC,
                self::VISIBILITY_PROTECTED
            ),
            'conflict' => array(
                self::CONFLICT_EXCEPTION,
                self::CONFLICT_OVERWRITE,
                self::CONFLICT_RENAME,
                self::CONFLICT_USE_EXISTING
            )
        );
    }

    public function getVisibility($filename, $hash)
    {
        $fileID = $this->getFileID($filename, $hash);
        $filesystem = $this->getFilesystem();
        return $filesystem->getVisibility($fileID);
    }

    public function getAsStream($filename, $hash, $variant = null)
    {
        $fileID = $this->getFileID($filename, $hash, $variant);
        return $this
            ->getFilesystem()
            ->readStream($fileID);
    }

    public function getAsString($filename, $hash, $variant = null)
    {
        $fileID = $this->getFileID($filename, $hash, $variant);
        return $this
            ->getFilesystem()
            ->read($fileID);
    }

    public function getAsURL($filename, $hash, $variant = null, $grant = true)
    {
        $fileID = $this->getFileID($filename, $hash, $variant);

        $filesystem = $this->getFilesystem();
        /** @var CombinedAdapter $adapter */
        $adapter = $filesystem->getAdapter();

        // Get visibility and generate appropriate URL
        switch ($filesystem->getVisibility($fileID)) {
            case AssetStore::VISIBILITY_PUBLIC:
                return $adapter->getPublicUrl($fileID);
            case AssetStore::VISIBILITY_PROTECTED:
                return $adapter->getProtectedUrl($fileID);
            default:
                // no file!
                return null;
        }
    }

    public function setFromLocalFile($path, $filename = null, $hash = null, $variant = null, $config = array())
    {
        // Validate this file exists
        if (!file_exists($path)) {
            throw new InvalidArgumentException("$path does not exist");
        }

        // Get filename to save to
        if (empty($filename)) {
            $filename = basename($path);
        }

        // Callback for saving content
        $callback = function (Filesystem $filesystem, $fileID, $config) use ($path) {
            // Read contents as string into flysystem
            $handle = fopen($path, 'r');
            if ($handle === false) {
                throw new InvalidArgumentException("$path could not be opened for reading");
            }
            $result = $filesystem->putStream($fileID, $handle, $config);
            fclose($handle);
            return $result;
        };

        // When saving original filename, generate hash
        if (!$variant) {
            $hash = sha1_file($path);
        }

        // Submit to conflict check
        return $this->writeWithCallback($callback, $filename, $hash, $variant, $config);
    }

    public function setFromString($data, $filename, $hash = null, $variant = null, $config = array())
    {
        // Callback for saving content
        $callback = function (Filesystem $filesystem, $fileID, $config) use ($data) {
            return $filesystem->put($fileID, $data, $config);
        };

        // When saving original filename, generate hash
        if (!$variant) {
            $hash = sha1($data);
        }

        // Submit to conflict check
        return $this->writeWithCallback($callback, $filename, $hash, $variant, $config);
    }

    public function setFromStream($stream, $filename, $hash = null, $variant = null, $config = array())
    {
        // If the stream isn't rewindable, write to a temporary filename
        if (!$this->isSeekableStream($stream)) {
            $path = $this->getStreamAsFile($stream);
            $result = $this->setFromLocalFile($path, $filename, $hash, $variant, $config);
            unlink($path);
            return $result;
        }

        // Callback for saving content
        $callback = function (Filesystem $filesystem, $fileID, $config) use ($stream) {
            return $filesystem->putStream($fileID, $stream, $config);
        };

        // When saving original filename, generate hash
        if (!$variant) {
            $hash = $this->getStreamSHA1($stream);
        }

        // Submit to conflict check
        return $this->writeWithCallback($callback, $filename, $hash, $variant, $config);
    }

    public function delete($filename, $hash)
    {
        $fileID = $this->getFileID($filename, $hash);
        $filesystem = $this->getFilesystem();

        // Delete all variants
        $deleted = false;
        foreach ($this->findVariants($fileID, $filesystem) as $nextID) {
            $filesystem->delete($nextID);
            $deleted = true;
        }

        return $deleted;
    }

    /**
     * Returns an iterable {@see Generator} of all files / variants for the given $fileID in the given $filesystem
     * This includes the empty (no) variant.
     *
     * @param string $fileID ID of original file to compare with.
     * @param Filesystem $filesystem
     * @return Generator
     */
    protected function findVariants($fileID, Filesystem $filesystem)
    {
        $dirname = ltrim(dirname($fileID), '.');
        foreach ($filesystem->listContents($dirname) as $next) {
            if ($next['type'] !== 'file') {
                continue;
            }
            $nextID = $next['path'];
            // Compare given file to target, omitting variant
            if ($fileID === $this->removeVariant($nextID)) {
                yield $nextID;
            }
        }
    }

    public function publish($filename, $hash)
    {
        $fileID = $this->getFileID($filename, $hash);
        $filesystem = $this->getFilesystem();
        $filesystem->setVisibility($fileID, AssetStore::VISIBILITY_PUBLIC);
    }

    public function protect($filename, $hash)
    {
        $fileID = $this->getFileID($filename, $hash);
        $filesystem = $this->getFilesystem();
        $filesystem->setVisibility($fileID, AssetStore::VISIBILITY_PROTECTED);
    }

    public function grant($filename, $hash)
    {
        // Noop in s3
    }

    public function revoke($filename, $hash)
    {
        // Noop in s3
    }

    public function canView($filename, $hash)
    {
        return true;
    }

    /**
     * Determine if a grant exists for the given FileID
     *
     * @param string $fileID
     * @return bool
     */
    protected function isGranted($fileID)
    {
        return true;
    }

    /**
     * get sha1 hash from stream
     *
     * @param resource $stream
     * @return string str1 hash
     */
    protected function getStreamSHA1($stream)
    {
        Util::rewindStream($stream);
        $context = hash_init('sha1');
        hash_update_stream($context, $stream);
        return hash_final($context);
    }

    /**
     * Get stream as a file
     *
     * @param resource $stream
     * @return string Filename of resulting stream content
     * @throws Exception
     */
    protected function getStreamAsFile($stream)
    {
        // Get temporary file and name
        $file = tempnam(sys_get_temp_dir(), 'ssflysystem');
        $buffer = fopen($file, 'w');
        if (!$buffer) {
            throw new Exception("Could not create temporary file");
        }

        // Transfer from given stream
        Util::rewindStream($stream);
        stream_copy_to_stream($stream, $buffer);
        if (! fclose($buffer)) {
            throw new Exception("Could not write stream to temporary file");
        }

        return $file;
    }

    /**
     * Determine if this stream is seekable
     *
     * @param resource $stream
     * @return bool True if this stream is seekable
     */
    protected function isSeekableStream($stream)
    {
        return Util::isSeekableStream($stream);
    }

    /**
     * Invokes the conflict resolution scheme on the given content, and invokes a callback if
     * the storage request is approved.
     *
     * @param callable $callback Will be invoked and passed a fileID if the file should be stored
     * @param string $filename Name for the resulting file
     * @param string $hash SHA1 of the original file content
     * @param string $variant Variant to write
     * @param array $config Write options. {@see AssetStore}
     * @return array Tuple associative array (Filename, Hash, Variant)
     * @throws Exception
     */
    protected function writeWithCallback($callback, $filename, $hash, $variant = null, $config = array())
    {
        // Validate arguments
        $reflection = new ReflectionFunction($callback);
        if (!$reflection->getNumberOfParameters() !== 3) {
            throw new InvalidArgumentException("Callback to writeWithCallback must take 3 args");
        }

        // Set default conflict resolution
        if (empty($config['conflict'])) {
            $conflictResolution = AssetStore::CONFLICT_OVERWRITE;
        } else {
            $conflictResolution = $config['conflict'];
        }

        // Validate parameters
        if ($variant && $conflictResolution === AssetStore::CONFLICT_RENAME) {
            // As variants must follow predictable naming rules, they should not be dynamically renamed
            throw new InvalidArgumentException("Rename cannot be used when writing variants");
        }
        if (!$filename) {
            throw new InvalidArgumentException("Filename is missing");
        }
        if (!$hash) {
            throw new InvalidArgumentException("File hash is missing");
        }

        $filename = $this->cleanFilename($filename);
        $fileID = $this->getFileID($filename, $hash, $variant);

        // Check conflict resolution scheme
        $resolvedID = $this->resolveConflicts($conflictResolution, $fileID);
        if ($resolvedID !== false) {
            $filesystem = $this->getFilesystem();

            // If visibility isn't declared, inherit from parent file (variant-excluded)
            if (!isset($config['visibility'])) {
                $mainID = $this->getFileID($filename, $hash);
                $visibility = $filesystem->getVisibility($mainID);
                if (!$visibility) {
                    $visibility = self::VISIBILITY_PUBLIC;
                }
                $config['visibility'] = $visibility;
            }

            // Submit and validate result
            $result = $callback($filesystem, $resolvedID, $config);
            if (!$result) {
                throw new Exception("Could not save {$filename}");
            }

            // in case conflict resolution renamed the file, return the renamed
            $filename = $this->getOriginalFilename($resolvedID);
        } elseif (empty($variant)) {
            // If deferring to the existing file, return the sha of the existing file,
            // unless we are writing a variant (which has the same hash value as its original file)
            $stream = $this
                ->getFilesystem()
                ->readStream($fileID);
            $hash = $this->getStreamSHA1($stream);
        }

        return array(
            'Filename' => $filename,
            'Hash' => $hash,
            'Variant' => $variant
        );
    }

    public function getMetadata($filename, $hash, $variant = null)
    {
        $fileID = $this->getFileID($filename, $hash, $variant);
        $filesystem = $this->getFilesystem();
        return $filesystem->getMetadata($fileID);
    }

    public function getMimeType($filename, $hash, $variant = null)
    {
        $fileID = $this->getFileID($filename, $hash, $variant);
        $filesystem = $this->getFilesystem();
        return $filesystem->getMimetype($fileID);
    }

    public function exists($filename, $hash, $variant = null)
    {
        $fileID = $this->getFileID($filename, $hash, $variant);
        $filesystem = $this->getFilesystem();
        return $filesystem->has($fileID);
    }

    /**
     * Determine the path that should be written to, given the conflict resolution scheme
     *
     * @param string $conflictResolution
     * @param string $fileID
     * @return string|false Safe filename to write to. If false, then don't write, and use existing file.
     * @throws Exception
     */
    protected function resolveConflicts($conflictResolution, $fileID)
    {
        // If overwrite is requested, simply put
        if ($conflictResolution === AssetStore::CONFLICT_OVERWRITE) {
            return $fileID;
        }

        // Otherwise, check if this exists
        $filesystem = $this->getFilesystem();
        $exists = $filesystem->has($fileID);
        if (!$exists) {
            return $fileID;
        }

        // Flysystem defaults to use_existing
        switch ($conflictResolution) {
            // Throw tantrum
            case static::CONFLICT_EXCEPTION: {
                throw new InvalidArgumentException("File already exists at path {$fileID}");
            }

            // Rename
            case static::CONFLICT_RENAME: {
                foreach ($this->fileGeneratorFor($fileID) as $candidate) {
                    if (!$filesystem->has($candidate)) {
                        return $candidate;
                    }
                }

                throw new InvalidArgumentException("File could not be renamed with path {$fileID}");
            }

            // Use existing file
            case static::CONFLICT_USE_EXISTING:
            default: {
                return false;
            }
        }
    }

    /**
     * Get an asset renamer for the given filename.
     *
     * @param string $fileID Adapter specific identifier for this file/version
     * @return AssetNameGenerator
     */
    protected function fileGeneratorFor($fileID)
    {
        return Injector::inst()->createWithArgs(AssetNameGenerator::class, array($fileID));
    }

    /**
     * Performs filename cleanup before sending it back.
     *
     * This name should not contain hash or variants.
     *
     * @param string $filename
     * @return string
     */
    protected function cleanFilename($filename)
    {
        // Since we use double underscore to delimit variants, eradicate them from filename
        return preg_replace('/_{2,}/', '_', $filename);
    }

    /**
     * Given a FileID, map this back to the original filename, trimming variant and hash
     *
     * @param string $fileID Adapter specific identifier for this file/version
     * @return string Filename for this file, omitting hash and variant
     */
    protected function getOriginalFilename($fileID)
    {
        // Remove variant
        $originalID = $this->removeVariant($fileID);

        // Remove hash (unless using legacy filenames, without hash)
        return preg_replace(
            '/(?<hash>[a-zA-Z0-9]{10}\\/)(?<name>[^\\/]+)$/',
            '$2',
            $originalID
        );
    }

    /**
     * Remove variant from a fileID
     *
     * @param string $fileID
     * @return string FileID without variant
     */
    protected function removeVariant($fileID)
    {
        // Check variant
        if (preg_match('/^(?<before>((?<!__).)+)__(?<variant>[^\\.]+)(?<after>.*)$/', $fileID, $matches)) {
            return $matches['before'] . $matches['after'];
        }
        // There is no variant, so return original value
        return $fileID;
    }

    /**
     * Map file tuple (hash, name, variant) to a filename to be used by flysystem
     *
     * The resulting file will look something like my/directory/EA775CB4D4/filename__variant.jpg
     *
     * @param string $filename Name of file
     * @param string $hash Hash of original file
     * @param string $variant (if given)
     * @return string Adapter specific identifier for this file/version
     */
    protected function getFileID($filename, $hash, $variant = null)
    {
        // Since we use double underscore to delimit variants, eradicate them from filename
        $filename = $this->cleanFilename($filename);
        $name = basename($filename);

        // Split extension
        $extension = null;
        if (($pos = strpos($name, '.')) !== false) {
            $extension = substr($name, $pos);
            $name = substr($name, 0, $pos);
        }

        // Inject hash just prior to the filename
        $fileID = substr($hash, 0, 10) . '/' . $name;

        // Add directory
        $dirname = ltrim(dirname($filename), '.');
        if ($dirname) {
            $fileID = $dirname . '/' . $fileID;
        }

        // Add variant
        if ($variant) {
            $fileID .= '__' . $variant;
        }

        // Add extension
        if ($extension) {
            $fileID .= $extension;
        }

        return $fileID;
    }
}
