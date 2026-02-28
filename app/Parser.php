<?php

namespace App;

use Exception;

final class Parser
{
    private const NUM_WORKERS = 2;
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
                // CHILD: process chunk, write directly to shared memory
                // Format: path\tdate\tcount\n per entry, 4-byte length prefix
                [$result, $dateStrById] = $this->processChunk($inputPath, $boundaries[$i], $boundaries[$i + 1]);
                $offset = 4;
                $writeBuffer = '';

                foreach ($result as $path => $dates) {
                    foreach ($dates as $dateId => $count) {
                        $date = $dateStrById[$dateId];
                        $writeBuffer .= $path . "\t" . $date . "\t" . $count . "\n";

                        if (strlen($writeBuffer) >= 524288) {
                            shmop_write($shmSegments[$i], $writeBuffer, $offset);
                            $offset += strlen($writeBuffer);
                            $writeBuffer = '';
                        }
                    }
                }

                if ($writeBuffer !== '') {
                    shmop_write($shmSegments[$i], $writeBuffer, $offset);
                    $offset += strlen($writeBuffer);
                }

                shmop_write($shmSegments[$i], pack('V', $offset - 4), 0);
                exit(0);
            }

            $pids[$i] = $pid;
        }

        // Wait for all children
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Merge results from shared memory
        $visits = [];

        foreach ($shmSegments as $shm) {
            $totalLen = unpack('V', shmop_read($shm, 0, 4))[1];
            $data = shmop_read($shm, 4, $totalLen);
            shmop_delete($shm);

            foreach (explode("\n", $data) as $line) {
                if ($line === '') {
                    continue;
                }

                [$path, $date, $count] = explode("\t", $line, 3);

                if (isset($visits[$path][$date])) {
                    $visits[$path][$date] += (int) $count;
                } else {
                    $visits[$path][$date] = (int) $count;
                }
            }
        }

        // Sort dates within each path
        foreach ($visits as &$dates) {
            ksort($dates, SORT_STRING);
        }

        unset($dates);

        // Build JSON manually — structure is always {path: {date: int}}
        $json = '{';
        $firstPath = true;

        foreach ($visits as $path => $dates) {
            if ($firstPath) {
                $firstPath = false;
            } else {
                $json .= ',';
            }

            $escapedPath = str_replace('/', '\\/', $path);
            $json .= "\n    \"$escapedPath\": {";
            $firstDate = true;

            foreach ($dates as $date => $count) {
                if ($firstDate) {
                    $firstDate = false;
                } else {
                    $json .= ',';
                }

                $json .= "\n        \"$date\": $count";
            }

            $json .= "\n    }";
        }

        $json .= "\n}";

        if ($wasGcEnabled) {
            gc_enable();
        }

        $out = fopen($outputPath, 'wb');
        fwrite($out, $json);
        fclose($out);
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
        $input = fopen($inputPath, 'rb');

        if ($input === false) {
            throw new Exception("Unable to open input file: {$inputPath}");
        }

        stream_set_read_buffer($input, 8 * 1024 * 1024);

        if ($startOffset > 0) {
            fseek($input, $startOffset);
        }

        $visits = [];
        $dateIdByStr = [];
        $dateStrById = [];
        $nextDateId = 0;
        $buffer = '';
        $bytesRemaining = $endOffset - $startOffset;

        while ($bytesRemaining > 0) {
            $readSize = min(self::CHUNK_SIZE, $bytesRemaining);
            $chunk = fread($input, $readSize);

            if ($chunk === false || $chunk === '') {
                break;
            }

            $bytesRemaining -= strlen($chunk);

            $buffer .= $chunk;
            $lastNewlinePosition = strrpos($buffer, "\n");

            if ($lastNewlinePosition === false) {
                continue;
            }

            $pos = 0;

            while ($pos < $lastNewlinePosition) {
                $commaPos = strpos($buffer, ',', $pos + 19);

                if ($commaPos === false) {
                    break;
                }

                $path = substr($buffer, $pos + 19, $commaPos - $pos - 19);
                $date = substr($buffer, $commaPos + 1, 10);
                $dateId = $dateIdByStr[$date] ?? null;

                if ($dateId === null) {
                    $dateId = $nextDateId;
                    $dateIdByStr[$date] = $dateId;
                    $dateStrById[$dateId] = $date;
                    ++$nextDateId;
                }

                $inner = &$visits[$path];

                if (isset($inner[$dateId])) {
                    ++$inner[$dateId];
                } else {
                    $inner[$dateId] = 1;
                }

                $pos = $commaPos + 27;
            }

            $buffer = substr($buffer, $lastNewlinePosition + 1);
        }

        // Handle remaining buffer (last partial line in this chunk)
        if ($buffer !== '' && $bytesRemaining <= 0) {
            $commaPos = strpos($buffer, ',', 19);

            if ($commaPos !== false) {
                $path = substr($buffer, 19, $commaPos - 19);
                $date = substr($buffer, $commaPos + 1, 10);
                $dateId = $dateIdByStr[$date] ?? null;

                if ($dateId === null) {
                    $dateId = $nextDateId;
                    $dateIdByStr[$date] = $dateId;
                    $dateStrById[$dateId] = $date;
                    ++$nextDateId;
                }

                $inner = &$visits[$path];

                if (isset($inner[$dateId])) {
                    ++$inner[$dateId];
                } else {
                    $inner[$dateId] = 1;
                }
            }
        }

        fclose($input);

        return [$visits, $dateStrById];
    }
}
