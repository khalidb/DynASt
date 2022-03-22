# DynASt
<<<<<<< HEAD
This repository contains the source code of DynAST, a tool for the discovery and maintenance of agree-sets against dynamic datasets.

To deploy the tool, you will need Java JDK 1.8 or later

The folder *DynAStPackage* contains the code java of Dynast including a subfolder of utility classes.

The folder *includingDependencies* include Dynast code but also the packages it depends on, as well as the Java libraries it utilizes. This folder also contains the subfolder *Resources*, which contains the datasets used for testing, which include the Flight dataset, the IRIS dataset, and the ADULT dataset.
The *Result* folder contains the files outputs by DynASt, which contains information about the performance for processing the batches of a given dataset.

The main Java class that needs to be edited to specify the input dataset is *IncrementalAgreeSetsMaintenance*. In particular, you will need to assign values to the following variables:
* file: the path of the dataset. 
* result_file: the name of the results in which to write information about the performance of DynASt
* batch_size: the size of the batch to consider
* number_of_batches: the number of batches to process before terminating. If the number of batches exceeds the size of the file, then the processing will terminates as soon as the last batch of the file is processed.
=======
This repository contains the source code of DynAST, a tool for the discovery and mainteannce of agreesets against dynamic datasets.

To deploy the tool, you will need Java JDK 1.8 or later

The folder *DynAStPackage* contains the code java of Dynast including a subfolder of utilities classes.

The folder *includingDependencies* include Dynast code but also the packages it depends on, as well as the Java libraries it utilizes. This folder also contains the subfolder *Resources*, which contains the datasets used for testing, which include the Flight dataset, the IRIS dataset and the ADULT dataset.
The *Result* folder contains the the files outputs by DynASt, which contains information about the performance for processing the batches of a given dataset.

The main Java class that need to be edited to specify the input dataset is *IncrementalAgreeSetsMaintenance*. In particular, you will need to assign values to the following variables:
* file: the path of the dataset. 
* result_file: the name of the results in which to write information about the performance of DynASt
* batch_size: the size of the batch to consider
* number_of_batches: the number of batches to process before terminating. If the number of batches exceeds the size of the file, then the processing will terminates as soon as the last batch of the file is processed.
>>>>>>> 9d91a09cae4513ce8923b7e2a6ee2d3919b6f805
