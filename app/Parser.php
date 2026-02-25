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
        $dateToId = [];
        $idToDate = [];
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
            $lineStart = 0;

            while (($newlinePosition = strpos($buffer, "\n", $lineStart)) !== false) {
                $dateStart = $newlinePosition - 25;
                $pathStart = $lineStart + $pathOffset;
                $path = substr($buffer, $pathStart, $dateStart - $pathStart - 1);
                $date = substr($buffer, $dateStart, 10);

                if (isset($dateToId[$date])) {
                    $dateId = $dateToId[$date];
                } else {
                    $dateId = count($idToDate);
                    $dateToId[$date] = $dateId;
                    $idToDate[$dateId] = $date;
                }

                if (isset($visits[$path][$dateId])) {
                    ++$visits[$path][$dateId];
                } else {
                    $visits[$path][$dateId] = 1;
                }

                $lineStart = $newlinePosition + 1;
            }

            if ($lineStart !== 0) {
                $buffer = substr($buffer, $lineStart);
            }
        }

        if ($buffer !== '') {
            $path = substr($buffer, $pathOffset, -26);
            $date = substr($buffer, -25, 10);
            if (isset($dateToId[$date])) {
                $dateId = $dateToId[$date];
            } else {
                $dateId = count($idToDate);
                $dateToId[$date] = $dateId;
                $idToDate[$dateId] = $date;
            }

            if (isset($visits[$path][$dateId])) {
                ++$visits[$path][$dateId];
            } else {
                $visits[$path][$dateId] = 1;
            }
        }

        fclose($input);

        asort($idToDate, SORT_STRING);

        foreach ($visits as &$countsByDateId) {
            $dates = [];

            foreach ($idToDate as $dateId => $date) {
                if (isset($countsByDateId[$dateId])) {
                    $dates[$date] = $countsByDateId[$dateId];
                }
            }

            $countsByDateId = $dates;
        }

        unset($countsByDateId);

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
