package com.org.bananas

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.metrics.Counter
import org.apache.beam.sdk.metrics.Distribution
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection


class ExtractWordsFn: DoFn<String, String>() {
    private val lineLenDist: Distribution = Metrics.distribution("ExtractWordsFn", "lineLenDistro")
    private val emptyLines: Counter = Metrics.counter("ExtractWordsFn", "emptyLines")
    private val tokenPattern: String =  "[^\\p{L}]+"

    @ProcessElement
    fun processElement(@Element line: String, receiver: OutputReceiver<String> ) {
        lineLenDist.update(line.length.toLong())

        if(line.trim().isEmpty()) {
            emptyLines.inc()
        }

        val words: List<String> = line.split(tokenPattern.toRegex())

        for(word in words) {
            if(!word.isEmpty()) {
                receiver.output(word)
            }
        }
    }
}

class CountWords: PTransform<PCollection<String>, PCollection<KV<String, Long>>>() {
    override fun expand(lines: PCollection<String>): PCollection<KV<String, Long>>? {
        val words: PCollection<String> = lines.apply(ParDo.of(ExtractWordsFn()))
        return words.apply<PCollection<KV<String, Long>>?>(Count.perElement())
    }
}

class FormatAsTextFn: SimpleFunction<KV<String, Long>, String>() {
    override fun apply(input: KV<String, Long>): String {
        return input.key +": " + input.value
    }
}

fun main(args: Array<String>) {
    val inputSource = "gs://apache-beam-samples/shakespeare/kinglear.txt";

    val options: PipelineOptions = PipelineOptionsFactory.create()

    println("pipeline options: $options")

    val p : Pipeline = Pipeline.create(options)

    println("pipeline: $p")

    val p0 = p.apply("read-input", TextIO.read().from(inputSource))
    val p1 = p0.apply(CountWords())
    val p2 = p1.apply(MapElements.via(FormatAsTextFn()))
    val p3 = p2.apply("write-output", TextIO.write().to("output"))

    p3.pipeline.run().waitUntilFinish()

    /*

    an alternative way to chain it:

    p.apply("read-input", TextIO.read().from(inputSource))
            .apply(CountWords())
            .apply(MapElements.via(FormatAsTextFn()))
            .apply("write-output", TextIO.write().to("output.txt"))

    p.run().waitUntilFinish()

    */

    println("finished.")
}

