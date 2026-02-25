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

        $pathToId = [];
        $paths = [];
        $counts = [];
        $hostPrefixLength = 19; // https://stitcher.io

        while (($line = fgets($input)) !== false) {
            $commaPosition = strpos($line, ',');
            $path = substr($line, $hostPrefixLength, $commaPosition - $hostPrefixLength);
            $date = substr($line, $commaPosition + 1, 10);

            if (isset($pathToId[$path])) {
                $pathId = $pathToId[$path];
            } else {
                $pathId = count($paths);
                $pathToId[$path] = $pathId;
                $paths[$pathId] = $path;
                $counts[$pathId] = [];
            }

            if (isset($counts[$pathId][$date])) {
                ++$counts[$pathId][$date];
                continue;
            }

            $counts[$pathId][$date] = 1;
        }

        fclose($input);

        foreach ($counts as &$dates) {
            ksort($dates);
        }

        unset($dates);

        $visits = [];

        foreach ($paths as $pathId => $path) {
            $visits[$path] = $counts[$pathId];
        }

        $encoded = json_encode($visits, JSON_PRETTY_PRINT);

        if ($encoded === false) {
            throw new Exception('Unable to encode JSON output');
        }

        if (file_put_contents($outputPath, $encoded) === false) {
            throw new Exception("Unable to write output file: {$outputPath}");
        }
    }
}
