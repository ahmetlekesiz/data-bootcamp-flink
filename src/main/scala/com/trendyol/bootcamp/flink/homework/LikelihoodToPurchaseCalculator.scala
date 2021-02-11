package com.trendyol.bootcamp.flink.homework

import java.time.Duration

import com.trendyol.bootcamp.flink.common._
import com.trendyol.bootcamp.flink.example.UserStats
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

import scala.Int.int2double

case class PurchaseLikelihood(userId: Int, productId: Int, likelihood: Double)

object LikelihoodToPurchaseCalculator {

  val l2pCoefficients = Map(
    AddToBasket         -> 0.4,
    RemoveFromBasket    -> -0.2,
    AddToFavorites      -> 0.7,
    RemoveFromFavorites -> -0.2,
    DisplayBasket       -> 0.5
  )

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(60000, CheckpointingMode.AT_LEAST_ONCE)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(100, 15000))

    // TODO Use processing time

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
                // TODO Get average of element's l2coefficients.
                elements.size
              )
            )
        }
      )
      .addSink(new PrintSinkFunction[PurchaseLikelihood])

    env.execute("Event Streamer")
  }

}
