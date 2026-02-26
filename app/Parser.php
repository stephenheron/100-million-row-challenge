<?php

namespace App;

use Exception;

final class Parser
{
    private const NUM_WORKERS = 2;
    private const SHM_SIZE = 16 * 1024 * 1024; // 16MB per worker
    private const CHUNK_SIZE = 2 * 1024 * 1024; // 2MB read buffer

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
                // CHILD: process chunk, write tab-delimited text to shared memory
                // Format: path\tdate\tcount\n per entry, 4-byte length prefix
                $result = $this->processChunk($inputPath, $boundaries[$i], $boundaries[$i + 1]);
                $parts = [];

                foreach ($result as $path => $dates) {
                    foreach ($dates as $date => $count) {
                        $parts[] = "$path\t$date\t$count";
                    }
                }

                $text = implode("\n", $parts);
                shmop_write($shmSegments[$i], pack('V', strlen($text)), 0);
                shmop_write($shmSegments[$i], $text, 4);
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

            if (preg_match_all('~https://stitcher\.io\K([^,\n]++),([0-9]{4}-[0-9]{2}-[0-9]{2})T[^\n]*+\n~', $buffer, $matches) !== 0) {
                $paths = $matches[1];
                $dates = $matches[2];
                $matchCount = count($paths);

                for ($i = 0; $i < $matchCount; ++$i) {
                    $inner = &$visits[$paths[$i]];

                    if (isset($inner[$dates[$i]])) {
                        ++$inner[$dates[$i]];
                    } else {
                        $inner[$dates[$i]] = 1;
                    }
                }
            }

            $buffer = substr($buffer, $lastNewlinePosition + 1);
        }

        // Handle remaining buffer (last partial line in this chunk)
        if ($buffer !== '' && $bytesRemaining <= 0) {
            if (preg_match('~https://stitcher\.io\K([^,\n]++),([0-9]{4}-[0-9]{2}-[0-9]{2})T~', $buffer, $match)) {
                $inner = &$visits[$match[1]];

                if (isset($inner[$match[2]])) {
                    ++$inner[$match[2]];
                } else {
                    $inner[$match[2]] = 1;
                }
            }
        }

        fclose($input);

        return $visits;
    }
}
