<?php

namespace App;

use Exception;

final class Parser
{
    private const NUM_WORKERS = 8;
    private const CHUNK_SIZE = 256 * 1024 * 1024; // 256MB read buffer

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '2G');

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

        // Create socket pairs for IPC
        $socketPairs = [];

        for ($i = 1; $i < self::NUM_WORKERS; ++$i) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);

            if ($pair === false) {
                throw new Exception("Unable to create socket pair for worker {$i}");
            }

            $socketPairs[$i] = $pair;
        }

        // Fork workers
        $pids = [];

        for ($i = 1; $i < self::NUM_WORKERS; ++$i) {
            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new Exception("Unable to fork worker {$i}");
            }

            if ($pid === 0) {
                // CHILD: close parent end, process chunk, write payload to socket.
                fclose($socketPairs[$i][0]);
                [$result, $pathStrById, $dateStrById] = $this->processChunk($inputPath, $boundaries[$i], $boundaries[$i + 1]);
                $payload = serialize([$result, $pathStrById, $dateStrById]);
                $sock = $socketPairs[$i][1];
                $len = strlen($payload);
                $written = 0;

                while ($written < $len) {
                    $w = fwrite($sock, substr($payload, $written, 65536));

                    if ($w === false) {
                        break;
                    }

                    $written += $w;
                }

                fclose($sock);
                exit(0);
            }

            // PARENT: close child end
            fclose($socketPairs[$i][1]);
            $pids[$i] = $pid;
        }

        // Parent processes the first chunk directly (avoids one IPC roundtrip)
        [$parentResult, $parentPathStrById, $parentDateStrById] = $this->processChunk($inputPath, $boundaries[0], $boundaries[1]);

        // Read results from child workers
        $payloads = [];

        for ($i = 1; $i < self::NUM_WORKERS; ++$i) {
            $data = stream_get_contents($socketPairs[$i][0]);
            fclose($socketPairs[$i][0]);
            $payloads[$i] = $data;
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Phase 1: Read all workers' data and build global ID mappings
        $workerData = [];
        $globalPathId = [];
        $globalPathStr = [];
        $nextGlobalPathId = 0;
        $globalDateId = [];
        $globalDateStr = [];
        $nextGlobalDateId = 0;

        $ingestWorker = static function (
            array $result,
            array $pathStrById,
            array $dateStrById,
            array &$workerData,
            array &$globalPathId,
            array &$globalPathStr,
            int &$nextGlobalPathId,
            array &$globalDateId,
            array &$globalDateStr,
            int &$nextGlobalDateId
        ): void {
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

            $workerData[] = [$result, $pathMap, $dateMap];
        };

        $ingestWorker(
            $parentResult,
            $parentPathStrById,
            $parentDateStrById,
            $workerData,
            $globalPathId,
            $globalPathStr,
            $nextGlobalPathId,
            $globalDateId,
            $globalDateStr,
            $nextGlobalDateId
        );

        foreach ($payloads as $data) {
            [$result, $pathStrById, $dateStrById] = unserialize($data);
            $ingestWorker(
                $result,
                $pathStrById,
                $dateStrById,
                $workerData,
                $globalPathId,
                $globalPathStr,
                $nextGlobalPathId,
                $globalDateId,
                $globalDateStr,
                $nextGlobalDateId
            );
        }

        // Phase 2: Merge into flat pre-allocated array (no isset checks needed)
        $numDates = $nextGlobalDateId;
        $flat = array_fill(0, $nextGlobalPathId * $numDates, 0);

        foreach ($workerData as [$result, $pathMap, $dateMap]) {
            foreach ($result as $localPathId => $dates) {
                $base = $pathMap[$localPathId] * $numDates;

                foreach ($dates as $localDateId => $count) {
                    $flat[$base + $dateMap[$localDateId]] += $count;
                }
            }
        }

        unset($workerData);

        // Phase 3: Sort dates and build final array for json_encode
        $sortedDateGids = range(0, $numDates - 1);
        usort($sortedDateGids, function ($a, $b) use ($globalDateStr) {
            return $globalDateStr[$a] <=> $globalDateStr[$b];
        });

        $visits = [];

        for ($gPathId = 0; $gPathId < $nextGlobalPathId; ++$gPathId) {
            $sorted = [];
            $base = $gPathId * $numDates;

            foreach ($sortedDateGids as $gDateId) {
                $count = $flat[$base + $gDateId];

                if ($count > 0) {
                    $sorted[$globalDateStr[$gDateId]] = $count;
                }
            }

            $visits[$globalPathStr[$gPathId]] = $sorted;
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
        $flat = array_fill(0, 300 * 1024, 0);
        $pathIdByStr = [];
        $pathStrById = [];
        $nextPathId = 0;
        $dateIdByStr = [];
        $dateStrById = [];
        $nextDateId = 0;
        $buffer = file_get_contents($inputPath, false, null, $startOffset, $endOffset - $startOffset);

        if ($buffer === false || $buffer === '') {
            return [[], $pathStrById, $dateStrById];
        }

        $lastNewlinePosition = strrpos($buffer, "\n");

        if ($lastNewlinePosition === false) {
            return [[], $pathStrById, $dateStrById];
        }

        $pos = 0;

        while ($pos < $lastNewlinePosition) {
            $pathStart = $pos + 19;
            $commaPos = strpos($buffer, ',', $pathStart);

            if ($commaPos === false) {
                break;
            }

            $path = substr($buffer, $pathStart, $commaPos - $pathStart);

            if (isset($pathIdByStr[$path])) {
                $pathId = $pathIdByStr[$path];
            } else {
                $pathId = $nextPathId;
                $pathIdByStr[$path] = $pathId;
                $pathStrById[] = $path;
                ++$nextPathId;
            }

            $date = substr($buffer, $commaPos + 1, 10);

            if (isset($dateIdByStr[$date])) {
                $dateId = $dateIdByStr[$date];
            } else {
                $dateId = $nextDateId;
                $dateIdByStr[$date] = $dateId;
                $dateStrById[] = $date;
                ++$nextDateId;
            }

            ++$flat[($pathId << 10) | $dateId];
            $pos = $commaPos + 27;
        }

        // Convert flat array back to nested for parent merge
        $visits = [];

        for ($pid = 0; $pid < $nextPathId; ++$pid) {
            $base = $pid << 10;
            $inner = [];

            for ($did = 0; $did < $nextDateId; ++$did) {
                $count = $flat[$base | $did];

                if ($count > 0) {
                    $inner[$did] = $count;
                }
            }

            if ($inner) {
                $visits[$pid] = $inner;
            }
        }

        return [$visits, $pathStrById, $dateStrById];
    }
}
