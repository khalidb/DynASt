/*
 * Copyright 2014 by the Metanome project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mp.naumann.algorithms.fd;

/**
 * Receives the results of a {@link FunctionalDependencyAlgorithm}.
 */
public interface FunctionalDependencyResultReceiver {

    /**
     * Receives a {@link FunctionalDependency} from a {@link FunctionalDependencyAlgorithm}.
     *
     * @param functionalDependency a found {@link de.metanome.algorithm_integration.results.FunctionalDependency}
     * @throws de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException if no
     *                                                                                          result
     *                                                                                          could
     *                                                                                          be received
     * @throws ColumnNameMismatchException                                                      if the
     *                                                                                          column
     *                                                                                          names
     *                                                                                          of the
     *                                                                                          result
     *                                                                                          does
     *                                                                                          not
     *                                                                                          match
     *                                                                                          the
     *                                                                                          column
     *                                                                                          names
     *                                                                                          of the
     *                                                                                          input
     */
    void receiveResult(FunctionalDependency functionalDependency);
}
