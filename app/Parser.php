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
        $hostPrefixLength = 19; // https://stitcher.io

        while (($line = fgets($input)) !== false) {
            $commaPosition = strpos($line, ',');
            $path = substr($line, $hostPrefixLength, $commaPosition - $hostPrefixLength);
            $date = substr($line, $commaPosition + 1, 10);

            if (! isset($visits[$path])) {
                $visits[$path] = [];
            }

            $pathVisits = &$visits[$path];

            if (isset($pathVisits[$date])) {
                ++$pathVisits[$date];
            } else {
                $pathVisits[$date] = 1;
            }

            unset($pathVisits);
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
