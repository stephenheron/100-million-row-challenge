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

        $pathToId = [];
        $idToPath = [];
        $visitsById = [];
        $dateToId = [];
        $idToDate = [];
        $nextDateId = 0;
        $pathOffset = 19; // strlen('https://stitcher.io')
        $chunkSize = 1024 * 1024;
        $buffer = '';
        $wasGcEnabled = gc_enabled();

        if ($wasGcEnabled) {
            gc_disable();
        }

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
            $lastNewlinePosition = strrpos($buffer, "\n");

            if ($lastNewlinePosition === false) {
                continue;
            }

            if (preg_match_all('~https://stitcher\.io([^,\n]+),(\d{4}-\d{2}-\d{2})T[^\n]*\n~', $buffer, $matches) !== 0) {
                $paths = $matches[1];
                $dates = $matches[2];
                $matchCount = count($paths);

                for ($i = 0; $i < $matchCount; ++$i) {
                    $path = $paths[$i];
                    $date = $dates[$i];

                    if (isset($pathToId[$path])) {
                        $pathId = $pathToId[$path];
                    } else {
                        $pathId = count($idToPath);
                        $pathToId[$path] = $pathId;
                        $idToPath[$pathId] = $path;
                        $visitsById[$pathId] = [];
                    }

                    $pathVisits = &$visitsById[$pathId];

                    if (isset($dateToId[$date])) {
                        $dateId = $dateToId[$date];
                    } else {
                        $dateId = $nextDateId;
                        ++$nextDateId;
                        $dateToId[$date] = $dateId;
                        $idToDate[$dateId] = $date;
                    }

                    if (isset($pathVisits[$dateId])) {
                        ++$pathVisits[$dateId];
                    } else {
                        $pathVisits[$dateId] = 1;
                    }

                    unset($pathVisits);
                }
            }

            $buffer = substr($buffer, $lastNewlinePosition + 1);
        }

        if ($buffer !== '') {
            $path = substr($buffer, $pathOffset, -26);
            $date = substr($buffer, -25, 10);

            if (isset($pathToId[$path])) {
                $pathId = $pathToId[$path];
            } else {
                $pathId = count($idToPath);
                $pathToId[$path] = $pathId;
                $idToPath[$pathId] = $path;
                $visitsById[$pathId] = [];
            }

            if (isset($dateToId[$date])) {
                $dateId = $dateToId[$date];
            } else {
                $dateId = $nextDateId;
                ++$nextDateId;
                $dateToId[$date] = $dateId;
                $idToDate[$dateId] = $date;
            }

            $pathVisits = &$visitsById[$pathId];

            if (isset($pathVisits[$dateId])) {
                ++$pathVisits[$dateId];
            } else {
                $pathVisits[$dateId] = 1;
            }

            unset($pathVisits);
        }

        fclose($input);

        $visits = [];

        foreach ($idToPath as $pathId => $path) {
            $countsByDateId = $visitsById[$pathId];
            $dates = [];

            foreach ($countsByDateId as $dateId => $count) {
                $dates[$idToDate[$dateId]] = $count;
            }

            if (count($dates) > 1) {
                ksort($dates, SORT_STRING);
            }

            $visits[$path] = $dates;
        }

        $encoded = json_encode($visits, JSON_PRETTY_PRINT);

        if ($encoded === false) {
            if ($wasGcEnabled) {
                gc_enable();
            }

            throw new Exception('Unable to encode JSON output');
        }

        if ($wasGcEnabled) {
            gc_enable();
        }

        if (file_put_contents($outputPath, $encoded) === false) {
            throw new Exception("Unable to write output file: {$outputPath}");
        }
    }
}
