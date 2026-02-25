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

        stream_set_read_buffer($input, 1024 * 1024);

        $visits = [];
        $pathOffset = 19; // strlen('https://stitcher.io')

        while (($line = fgets($input)) !== false) {
            $path = substr($line, $pathOffset, -27);
            $date = substr($line, -26, 10);

            if (isset($visits[$path][$date])) {
                ++$visits[$path][$date];
                continue;
            }

            $visits[$path][$date] = 1;
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
