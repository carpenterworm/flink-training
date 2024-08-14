/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.EnrichedRide;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.GeoUtils;

import org.apache.flink.util.Collector;
import org.joda.time.Interval;
import org.joda.time.Minutes;

//import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;

/**
 * The Ride Cleansing exercise from the Flink training.
 *
 * <p>The task of this exercise is to filter a data stream of taxi ride records to keep only rides
 * that both start and end within New York City. The resulting stream should be printed.
 */
public class RideCleansingExercise {

    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<EnrichedRide> sink;


    /** Creates a job using the source and sink provided. */
    public RideCleansingExercise(SourceFunction<TaxiRide> source, SinkFunction<EnrichedRide> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        RideCleansingExercise job =
                new RideCleansingExercise(new TaxiRideGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * Creates and executes the long rides pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the pipeline
//        env.addSource(source).filter(new NYCFilter()).addSink(sink);
        DataStream<EnrichedRide> enrichedNYCRides = env.addSource(source).filter(new NYCFilter()).map(new Enrichment());
//        env.addSource(source).flatMap(new NYCEnrichment()).addSink(sink);

        DataStream<Tuple2<Integer, Minutes>> minutesByStartCell = enrichedNYCRides
                .flatMap(new FlatMapFunction<EnrichedRide, Tuple2<Integer, Minutes>>() {
                    @Override
                    public void flatMap(EnrichedRide ride,
                                        Collector<Tuple2<Integer, Minutes>> out) throws Exception {
                        if (!ride.isStart) {
                            Interval rideInterval = new Interval(ride.startTime.toEpochMilli(), ride.endTime.toEpochMilli());
                            Minutes duration = rideInterval.toDuration().toStandardMinutes();
                            out.collect(new Tuple2<>(ride.startCell, duration));
                        }
                    }
                }).keyBy(value -> value.f0).maxBy(1);

        minutesByStartCell.print();

        // run the pipeline and return the result
        return env.execute("Taxi Ride Cleansing");
    }

    /** Keep only those rides and both start and end in NYC. */
    public static class NYCFilter implements FilterFunction<TaxiRide> {
        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {
            if (taxiRide.isStart) {
                return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat);
            } else {
                return GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
            }
//            throw new MissingSolutionException();
        }
    }

    /**
     * Enrichment of the ride record with additional information.
     *
     * <p>This is a helper class that is not part of the solution.
     */
    public static class Enrichment implements MapFunction<TaxiRide, EnrichedRide> {

        @Override
        public EnrichedRide map(TaxiRide taxiRide) throws Exception {
            return new EnrichedRide(taxiRide);
        }
    }


    public static class NYCEnrichment implements FlatMapFunction<TaxiRide, EnrichedRide> {
        @Override
        public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> out) throws Exception {
            FilterFunction<TaxiRide> valid = new NYCFilter();
            if (valid.filter(taxiRide)) {
                out.collect(new EnrichedRide(taxiRide));
            }
        }
    }
}
