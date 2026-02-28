<?php

namespace App;

use Exception;

final class Parser
{
    private const NUM_WORKERS = 8;

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', -1);

        gc_disable();

        $fileSize = filesize($inputPath);

        if ($fileSize === false) {
            throw new Exception("Unable to get file size: {$inputPath}");
        }

        // Calculate chunk boundaries aligned to newlines
        $boundaries = $this->calculateBoundaries($inputPath, $fileSize);

        // Unique prefix for temp files
        $tmpPrefix = sys_get_temp_dir() . '/parser_' . getmypid() . '_';

        // Fork workers — each writes results to a temp file (no sockets needed)
        $pids = [];

        for ($i = 1; $i < self::NUM_WORKERS; ++$i) {
            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new Exception("Unable to fork worker {$i}");
            }

            if ($pid === 0) {
                // CHILD: process chunk, write binary result to temp file
                [$workerFlat, $nextPathId, $nextDateId, $pathStrById, $dateStrById] = $this->processChunk($inputPath, $boundaries[$i], $boundaries[$i + 1]);

                $tmpFile = $tmpPrefix . $i;
                $fp = fopen($tmpFile, 'wb');

                // Header: nextPathId, nextDateId as 4-byte unsigned ints
                fwrite($fp, pack('VV', $nextPathId, $nextDateId));

                // Flat array as packed binary — only the used region (nextPathId * 1024 entries)
                $usedSize = $nextPathId << 10;
                if ($usedSize > 0) {
                    // Pack in chunks to avoid massive argument list to pack()
                    $chunkSize = 8192;
                    for ($offset = 0; $offset < $usedSize; $offset += $chunkSize) {
                        $slice = array_slice($workerFlat, $offset, min($chunkSize, $usedSize - $offset));
                        fwrite($fp, pack('V*', ...$slice));
                    }
                }

                // String tables — small, serialize is fine for these
                $stringData = serialize([$pathStrById, $dateStrById]);
                fwrite($fp, $stringData);

                fclose($fp);
                exit(0);
            }

            $pids[$i] = $pid;
        }

        // Parent processes the first chunk directly (avoids one IPC roundtrip)
        [$parentFlat, $parentNextPathId, $parentNextDateId, $parentPathStrById, $parentDateStrById] = $this->processChunk($inputPath, $boundaries[0], $boundaries[1]);

        // Wait for all children to finish writing
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Phase 1: Build global ID mappings
        $workerData = [];
        $globalPathId = [];
        $globalPathStr = [];
        $nextGlobalPathId = 0;
        $globalDateId = [];
        $globalDateStr = [];
        $nextGlobalDateId = 0;

        // Process parent result inline
        $pathMap = [];
        foreach ($parentPathStrById as $localId => $str) {
            $gid = $globalPathId[$str] ?? null;
            if ($gid === null) {
                $gid = $nextGlobalPathId++;
                $globalPathId[$str] = $gid;
                $globalPathStr[$gid] = $str;
            }
            $pathMap[$localId] = $gid;
        }
        $dateMap = [];
        foreach ($parentDateStrById as $localId => $str) {
            $gid = $globalDateId[$str] ?? null;
            if ($gid === null) {
                $gid = $nextGlobalDateId++;
                $globalDateId[$str] = $gid;
                $globalDateStr[$gid] = $str;
            }
            $dateMap[$localId] = $gid;
        }
        $workerData[] = [$parentFlat, $parentNextPathId, $parentNextDateId, $pathMap, $dateMap];

        unset($parentFlat, $parentPathStrById, $parentDateStrById);

        // Read child results from temp files — binary unpack instead of unserialize
        for ($i = 1; $i < self::NUM_WORKERS; ++$i) {
            $tmpFile = $tmpPrefix . $i;
            $raw = file_get_contents($tmpFile);
            unlink($tmpFile);

            // Read header
            $header = unpack('VnextPathId/VnextDateId', $raw, 0);
            $workerNextPathId = $header['nextPathId'];
            $workerNextDateId = $header['nextDateId'];

            // Read flat array from binary
            $headerSize = 8; // 2 × 4 bytes
            $usedSize = $workerNextPathId << 10;
            $flatBytes = $usedSize * 4; // 4 bytes per uint32

            $workerFlat = [];
            if ($usedSize > 0) {
                $workerFlat = array_values(unpack('V*', substr($raw, $headerSize, $flatBytes)));
            }

            // Read string tables from remainder
            $stringOffset = $headerSize + $flatBytes;
            [$pathStrById, $dateStrById] = unserialize(substr($raw, $stringOffset));

            unset($raw);

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
            $workerData[] = [$workerFlat, $workerNextPathId, $workerNextDateId, $pathMap, $dateMap];
        }

        // Phase 2: Merge flat arrays directly — no nested array overhead
        $numDates = $nextGlobalDateId;
        $flat = array_fill(0, $nextGlobalPathId * $numDates, 0);

        foreach ($workerData as [$workerFlat, $workerNextPathId, $workerNextDateId, $pathMap, $dateMap]) {
            for ($pid = 0; $pid < $workerNextPathId; ++$pid) {
                $localBase = $pid << 10;
                $globalBase = $pathMap[$pid] * $numDates;

                for ($did = 0; $did < $workerNextDateId; ++$did) {
                    $count = $workerFlat[$localBase | $did];

                    if ($count > 0) {
                        $flat[$globalBase + $dateMap[$did]] += $count;
                    }
                }
            }
        }

        unset($workerData);

        // Phase 3: Sort dates and build final array for json_encode
        $sortedDateMap = $globalDateStr;
        asort($sortedDateMap);

        $visits = [];

        for ($gPathId = 0; $gPathId < $nextGlobalPathId; ++$gPathId) {
            $sorted = [];
            $base = $gPathId * $numDates;

            foreach ($sortedDateMap as $gDateId => $dateStr) {
                $count = $flat[$base + $gDateId];

                if ($count > 0) {
                    $sorted[$dateStr] = $count;
                }
            }

            $visits[$globalPathStr[$gPathId]] = $sorted;
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
        $flat = \array_fill(0, 300 * 1024, 0);
        $pathIdByStr = [];
        $pathStrById = [];
        $nextPathId = 0;
        $dateIdByStr = [];
        $dateStrById = [];
        $nextDateId = 0;
        $buffer = \file_get_contents($inputPath, false, null, $startOffset, $endOffset - $startOffset);

        if ($buffer === false || $buffer === '') {
            return [$flat, 0, 0, $pathStrById, $dateStrById];
        }

        $lastNewlinePosition = \strlen($buffer) - 1;

        if ($buffer[$lastNewlinePosition] !== "\n") {
            $lastNewlinePosition = \strrpos($buffer, "\n");

            if ($lastNewlinePosition === false) {
                return [$flat, 0, 0, $pathStrById, $dateStrById];
            }
        }

        $pos = 0;

        while ($pos < $lastNewlinePosition) {
            $commaPos = \strpos($buffer, ',', $pos + 19);
            $path = \substr($buffer, $pos + 19, $commaPos - $pos - 19);

            $pathId = $pathIdByStr[$path] ?? null;

            if ($pathId === null) {
                $pathId = $nextPathId;
                $pathIdByStr[$path] = $pathId;
                $pathStrById[] = $path;
                ++$nextPathId;
            }

            $date = \substr($buffer, $commaPos + 1, 10);

            $dateId = $dateIdByStr[$date] ?? null;

            if ($dateId === null) {
                $dateId = $nextDateId;
                $dateIdByStr[$date] = $dateId;
                $dateStrById[] = $date;
                ++$nextDateId;
            }

            ++$flat[($pathId << 10) | $dateId];
            $pos = $commaPos + 27;
        }

        // Return flat array directly — no nested conversion
        return [$flat, $nextPathId, $nextDateId, $pathStrById, $dateStrById];
    }
}