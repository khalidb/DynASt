package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.benchmark.speed.Benchmark;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.utils.IncrementalFDResultListener;
import org.mp.naumann.data.ResourceConnector;
import org.mp.naumann.database.ConnectionException;

import java.util.List;
import java.util.logging.Level;

public class IncrementalFDDemo {

    private static final IncrementalFDRunConfiguration sample = new IncrementalFDRunConfiguration(
            "deletes.deletesample.csv",
            "",
            "test.deletesample",
            1800,
            ResourceConnector.TEST,
            ","
    );

    private static final IncrementalFDRunConfiguration philippSample = new IncrementalFDRunConfiguration(
            "simple.csv",
            "",
            "simple",
            1800,
            ResourceConnector.BASELINE,
            ","
    );

    private static final IncrementalFDRunConfiguration countries = new IncrementalFDRunConfiguration(
            "deletes.countries.csv",
            "",
            "countries",
            1800,
            ResourceConnector.BASELINE,
            ","
    );

    private static final IncrementalFDRunConfiguration adultInsert = new IncrementalFDRunConfiguration(
            "inserts.adult.csv",
            "",
            "benchmark.adult",
            5000,
            ResourceConnector.BENCHMARK,
            ","
    );

    private static final IncrementalFDRunConfiguration adult = new IncrementalFDRunConfiguration(
            "deletes.adult.csv",
            "",
            "benchmark.adult",
            1800,
            ResourceConnector.BENCHMARK,
            ","
    );

    
    private static final IncrementalFDRunConfiguration test_simple = new IncrementalFDRunConfiguration(
            "small.csv",
            "",
            "small",
            1800,
            ResourceConnector.BENCHMARK,
            ","
    );
    
    private static final IncrementalFDRunConfiguration iris = new IncrementalFDRunConfiguration(
            "insert.iris-num.csv",
            "",
            "iris-num",
            10,
            ResourceConnector.BENCHMARK,
            ","
    );
    
    private static final IncrementalFDRunConfiguration num_bridges = new IncrementalFDRunConfiguration(
            "num_bridges_full_deletes.csv",
            "",
            "num_bridges_full",
            10,
            ResourceConnector.BENCHMARK,
            ","
    );
    
    private static final IncrementalFDRunConfiguration iris_conf2 = new IncrementalFDRunConfiguration(
            "insert.iris-num.csv",
            "",
            "iris-num",
            40,
            ResourceConnector.BENCHMARK,
            ","
    );
    
    private static final IncrementalFDRunConfiguration bridges1 = new IncrementalFDRunConfiguration(
            "insert.bridges.csv",
            "",
            "bridges",
            50,
            ResourceConnector.BENCHMARK,
            ","
    );
    
    private static final IncrementalFDRunConfiguration adult1 = new IncrementalFDRunConfiguration(
            "num_dataset_insert.csv",
            "",
            "num_dataset",
            500,
            ResourceConnector.BENCHMARK,
            ","
    );
    
    private static final IncrementalFDRunConfiguration flight = new IncrementalFDRunConfiguration(
            "num_flight_1k_insert_1.csv",
            "",
            "num_flight_1k",
            100,
            ResourceConnector.BENCHMARK,
            ","
    );
    
    private static final IncrementalFDRunConfiguration flight_delete = new IncrementalFDRunConfiguration(
            "num_flight_1k_d_deletes.csv",
            "",
            "num_flight_1k_d",
            1,
            ResourceConnector.BENCHMARK,
            ","
    );
    
    private static final IncrementalFDRunConfiguration bridges = new IncrementalFDRunConfiguration(
            "deletes.bridges.csv",
            "",
            "test.bridges",
            200,
            ResourceConnector.TEST,
            ","
    );

    public static void main(String[] args) throws ClassNotFoundException, ConnectionException, AlgorithmExecutionException {
        FDLogger.setLevel(Level.FINE);

       // IncrementalFDConfiguration configuration = IncrementalFDConfiguration.LATEST;
      IncrementalFDConfiguration configuration = new IncrementalFDConfiguration("custom")
                .addPruningStrategy(IncrementalFDConfiguration.PruningStrategy.DELETE_ANNOTATIONS)
                .setDepthFirst(true)
//                .addPruningStrategy(PruningStrategy.BLOOM_ADVANCED)
                ;
        //IncrementalFDRunConfiguration runConfig = adult;
        //IncrementalFDRunConfiguration runConfig = test_simple;
      //IncrementalFDRunConfiguration runConfig = iris_conf2;
      //IncrementalFDRunConfiguration runConfig = bridges1;
      //IncrementalFDRunConfiguration runConfig = adult1;
      //IncrementalFDRunConfiguration runConfig = flight;
      //IncrementalFDRunConfiguration runConfig = num_bridges;
      IncrementalFDRunConfiguration runConfig = flight_delete;

        Benchmark.enableAll();
        //Benchmark.addEventListener(FDLogger::info);

        IncrementalFDRunner runner = new IncrementalFDRunner() {
            @Override
            public void afterInitial(List<FunctionalDependency> dependencyList) {
                FDLogger.log(Level.INFO, String.format("Original FD count: %s", dependencyList.size()));
                FDLogger.log(Level.INFO, String.format("Batch size: %s", runConfig.getBatchSize()));
                FDLogger.log(Level.FINEST, "\n");
                //dependencyList.forEach(fd -> FDLogger.log(Level.FINEST, fd.toString()));
                dependencyList.forEach(fd -> FDLogger.log(Level.INFO, fd.toString()));
                
            }

            @Override
            public void afterIncremental(IncrementalFDResultListener listener) {

                // output results
                FDLogger.log(Level.INFO, String.format("Total performed validations: %s", listener.getValidationCount()));
                FDLogger.log(Level.INFO, String.format("Total pruned validations: %s", listener.getPrunedCount()));
                FDLogger.log(Level.INFO, String.format("Final FD count: %s", listener.getFDs().size()));
                FDLogger.log(Level.FINEST, "\n");
               // listener.getFDs().forEach(f -> FDLogger.log(Level.FINEST, f.toString()));
                listener.getFDs().forEach(f -> FDLogger.log(Level.INFO, f.toString()));
                
                
               
            }
        };
        runner.run(runConfig, configuration);
        
        
        
        
    }

}
