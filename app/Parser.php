<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $input = fopen($inputPath, 'rb');

        if ($input === false) {
            throw new Exception("Unable to open input file: {$inputPath}");
        }

        stream_set_read_buffer($input, 8 * 1024 * 1024);

        $visits = [];
        $pathOffset = 19; // strlen('https://stitcher.io')
        $chunkSize = 1024 * 1024;
        $buffer = '';

        while (! feof($input)) {
            $chunk = fread($input, $chunkSize);

            if ($chunk === false) {
                fclose($input);
                throw new Exception("Unable to read input file: {$inputPath}");
            }

            if ($chunk === '') {
                break;
            }

            $buffer .= $chunk;
            $start = 0;

            while (($newlinePosition = strpos($buffer, "\n", $start)) !== false) {
                $dateStart = $newlinePosition - 25;
                $path = substr($buffer, $start + $pathOffset, $dateStart - $start - $pathOffset - 1);
                $date = substr($buffer, $dateStart, 10);

                if (isset($visits[$path][$date])) {
                    ++$visits[$path][$date];
                } else {
                    $visits[$path][$date] = 1;
                }

                $start = $newlinePosition + 1;
            }

            if ($start !== 0) {
                $buffer = substr($buffer, $start);
            }
        }

        if ($buffer !== '') {
            $bufferLength = strlen($buffer);

            if ($bufferLength > 26) {
                $dateStart = $bufferLength - 25;
                $path = substr($buffer, $pathOffset, $dateStart - $pathOffset - 1);
                $date = substr($buffer, $dateStart, 10);

                if (isset($visits[$path][$date])) {
                    ++$visits[$path][$date];
                } else {
                    $visits[$path][$date] = 1;
                }
            }
        }

        fclose($input);

        foreach ($visits as &$dates) {
            ksort($dates);
        }

        unset($dates);

        $encoded = json_encode($visits, JSON_PRETTY_PRINT);

        if ($encoded === false) {
            throw new Exception('Unable to encode JSON output');
        }

        if (file_put_contents($outputPath, $encoded) === false) {
            throw new Exception("Unable to write output file: {$outputPath}");
        }
    }
}
