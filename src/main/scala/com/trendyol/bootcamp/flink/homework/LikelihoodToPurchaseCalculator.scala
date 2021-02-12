package com.trendyol.bootcamp.flink.homework

import java.time.Duration

import com.trendyol.bootcamp.flink.common._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

case class PurchaseLikelihood(userId: Int, productId: Int, likelihood: Double)

object LikelihoodToPurchaseCalculator {

  // Related coefficient of each event to purchase or not.
  val l2pCoefficients = Map(
    AddToBasket         -> 0.4,
    RemoveFromBasket    -> -0.2,
    AddToFavorites      -> 0.7,
    RemoveFromFavorites -> -0.2,
    DisplayBasket       -> 0.5
  )

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    // Get execution environment and configure checkpoint and restart strategy settings.
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(60000, CheckpointingMode.AT_LEAST_ONCE)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(100, 15000))

    // Source: RandomEventSource, Sink: PrintSinkFunction
    // Firstly, filter events which have effect on the likelihood ratio of purchase.
    // Then, group the elements according to their userId and productId by using keyBy.
    // After that process, we will have group of userId and productId pairs.
    val keyedStream = env
      .addSource(new RandomEventSource)
      .filter(e => List(AddToBasket, AddToFavorites, RemoveFromBasket, RemoveFromFavorites, DisplayBasket).contains(e.eventType))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(10))
          .withTimestampAssigner(
            new SerializableTimestampAssigner[Event] {
              override def extractTimestamp(element: Event, recordTimestamp: Long): Long =
                element.timestamp
            }
          )
      )
      .keyBy(e => (e.userId, e.productId))

    // Set Time Window Interval as 20 seconds.
    keyedStream
      .window(TumblingEventTimeWindows.of(Time.seconds(20)))
      .process(
        new ProcessWindowFunction[Event, PurchaseLikelihood, (Int, Int), TimeWindow] {
          override def process(
                                key: (Int, Int),
                                context: Context,
                                elements: Iterable[Event],
                                out: Collector[PurchaseLikelihood]
                              ): Unit =
            out.collect(
              PurchaseLikelihood(
                key._1,
                key._2,
                // Map Event of Elements to L2pCoefficient List, then get average of them for final L2pCoefficient.
                // Let say, we have AddToFavorites and AddToBasket for one of our userID, productID.
                // The final likelihood to purchase will be (0.7 + 0.4)/2 = 0.55
                elements
                  .map(e => l2pCoefficients.getOrElse(e.eventType, 0).asInstanceOf[Double])
                  .sum
                  / elements.size
              )
            )
        }
      )
      .addSink(new PrintSinkFunction[PurchaseLikelihood])

    // Set job name.
    env.execute("Likelihood to Purchase Calculator")
  }

}
