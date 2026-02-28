<?php

namespace App;

use Exception;

final class Parser
{
    private const NUM_WORKERS = 8;
    private const SHM_SIZE = 32 * 1024 * 1024; // 32MB per worker
    private const CHUNK_SIZE = 4 * 1024 * 1024; // 4MB read buffer

    public function parse(string $inputPath, string $outputPath): void
    {
        $wasGcEnabled = gc_enabled();

        if ($wasGcEnabled) {
            gc_disable();
        }

        $fileSize = filesize($inputPath);

        if ($fileSize === false) {
            throw new Exception("Unable to get file size: {$inputPath}");
        }

        // Calculate chunk boundaries aligned to newlines
        $boundaries = $this->calculateBoundaries($inputPath, $fileSize);

        // Create shared memory segments
        $shmSegments = [];

        for ($i = 0; $i < self::NUM_WORKERS; ++$i) {
            $shmKey = ftok($inputPath, chr($i));
            $shm = shmop_open($shmKey, 'c', 0644, self::SHM_SIZE);

            if ($shm === false) {
                throw new Exception("Unable to create shared memory segment for worker {$i}");
            }

            $shmSegments[$i] = $shm;
        }

        // Fork workers
        $pids = [];

        for ($i = 0; $i < self::NUM_WORKERS; ++$i) {
            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new Exception("Unable to fork worker {$i}");
            }

            if ($pid === 0) {
                // CHILD: process chunk, write binary payload to shared memory.
                [$result, $pathStrById, $dateStrById] = $this->processChunk($inputPath, $boundaries[$i], $boundaries[$i + 1]);
                $payload = function_exists('igbinary_serialize')
                    ? igbinary_serialize([$result, $pathStrById, $dateStrById])
                    : serialize([$result, $pathStrById, $dateStrById]);
                $payloadLen = strlen($payload);

                if ($payloadLen > self::SHM_SIZE - 4) {
                    exit(1);
                }

                shmop_write($shmSegments[$i], pack('V', $payloadLen), 0);
                shmop_write($shmSegments[$i], $payload, 4);
                exit(0);
            }

            $pids[$i] = $pid;
        }

        // Wait for all children
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Merge results into global int-key arrays for speed
        $globalVisits = [];
        $globalPathId = [];
        $globalPathStr = [];
        $nextGlobalPathId = 0;
        $globalDateId = [];
        $globalDateStr = [];
        $nextGlobalDateId = 0;

        foreach ($shmSegments as $shm) {
            $totalLen = unpack('V', shmop_read($shm, 0, 4))[1];
            $data = shmop_read($shm, 4, $totalLen);
            shmop_delete($shm);
            [$result, $pathStrById, $dateStrById] = function_exists('igbinary_unserialize')
                ? igbinary_unserialize($data)
                : unserialize($data);

            // Map this worker's local IDs to global IDs
            $pathMap = [];

            foreach ($pathStrById as $localId => $str) {
                $gid = $globalPathId[$str] ?? null;

                if ($gid === null) {
                    $gid = $nextGlobalPathId++;
                    $globalPathId[$str] = $gid;
                    $globalPathStr[$gid] = $str;
                }

                $pathMap[$localId] = $gid;
            }

            $dateMap = [];

            foreach ($dateStrById as $localId => $str) {
                $gid = $globalDateId[$str] ?? null;

                if ($gid === null) {
                    $gid = $nextGlobalDateId++;
                    $globalDateId[$str] = $gid;
                    $globalDateStr[$gid] = $str;
                }

                $dateMap[$localId] = $gid;
            }

            // Merge using global int IDs only
            foreach ($result as $localPathId => $dates) {
                $gp = $pathMap[$localPathId];
                $inner = &$globalVisits[$gp];

                foreach ($dates as $localDateId => $count) {
                    $gd = $dateMap[$localDateId];

                    if (isset($inner[$gd])) {
                        $inner[$gd] += $count;
                    } else {
                        $inner[$gd] = $count;
                    }
                }
            }
        }

        // Sort global date IDs by date string
        $sortedDateGids = array_keys($globalDateStr);
        usort($sortedDateGids, function ($a, $b) use ($globalDateStr) {
            return $globalDateStr[$a] <=> $globalDateStr[$b];
        });

        // Build final string-keyed array for json_encode
        $visits = [];

        foreach ($globalVisits as $gPathId => $dates) {
            $path = $globalPathStr[$gPathId];
            $sorted = [];

            foreach ($sortedDateGids as $gDateId) {
                if (isset($dates[$gDateId])) {
                    $sorted[$globalDateStr[$gDateId]] = $dates[$gDateId];
                }
            }

            $visits[$path] = $sorted;
        }

        if ($wasGcEnabled) {
            gc_enable();
        }

        file_put_contents($outputPath, json_encode($visits, JSON_PRETTY_PRINT));
    }

    private function calculateBoundaries(string $inputPath, int $fileSize): array
    {
        $boundaries = [0];

        $fp = fopen($inputPath, 'rb');

        if ($fp === false) {
            throw new Exception("Unable to open input file: {$inputPath}");
        }

        for ($i = 1; $i < self::NUM_WORKERS; ++$i) {
            $approxPos = (int) ($i * $fileSize / self::NUM_WORKERS);
            fseek($fp, $approxPos);

            // Scan forward to next newline
            $line = fgets($fp);

            if ($line === false) {
                // Past EOF, just use fileSize
                $boundaries[] = $fileSize;
            } else {
                $boundaries[] = ftell($fp);
            }
        }

        $boundaries[] = $fileSize;
        fclose($fp);

        return $boundaries;
    }

    private function processChunk(string $inputPath, int $startOffset, int $endOffset): array
    {
        $buffer = file_get_contents($inputPath, false, null, $startOffset, $endOffset - $startOffset);

        $visits = [];
        $pathIdByStr = [];
        $pathStrById = [];
        $nextPathId = 0;
        $dateIdByStr = [];
        $dateStrById = [];
        $nextDateId = 0;

        $lastNewlinePosition = strrpos($buffer, "\n");

        if ($lastNewlinePosition === false) {
            return [$visits, $pathStrById, $dateStrById];
        }

        $pos = 0;

        while ($pos < $lastNewlinePosition) {
            $pathStart = $pos + 19;
            $commaPos = strpos($buffer, ',', $pathStart);

            if ($commaPos === false) {
                break;
            }

            $path = substr($buffer, $pathStart, $commaPos - $pathStart);
            $pathId = $pathIdByStr[$path] ?? null;

            if ($pathId === null) {
                $pathId = $nextPathId;
                $pathIdByStr[$path] = $pathId;
                $pathStrById[$pathId] = $path;
                ++$nextPathId;
            }

            $date = substr($buffer, $commaPos + 1, 10);
            $dateId = $dateIdByStr[$date] ?? null;

            if ($dateId === null) {
                $dateId = $nextDateId;
                $dateIdByStr[$date] = $dateId;
                $dateStrById[$dateId] = $date;
                ++$nextDateId;
            }

            $inner = &$visits[$pathId];

            if (isset($inner[$dateId])) {
                ++$inner[$dateId];
            } else {
                $inner[$dateId] = 1;
            }

            $pos = $commaPos + 27;
        }

        return [$visits, $pathStrById, $dateStrById];
    }
}
