/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 * (c) Copyright contributors to the Substrait project
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
 * 
 * This file has been modified from the original version.
 */

package com.palantir.gradle.graal;

import java.io.File;
import java.util.AbstractSet;
import java.util.HashSet;
import java.util.List;

public final class FileUtil {
  public static String getFirstFromDirectory(final File directory, final List<String> searchList) {
    if (!directory.exists()) {
      return null;
    }

    final AbstractSet<String> subDirectoriesNames = new HashSet<String>();
    final File[] subDirectories = directory.listFiles(File::isDirectory);
    for (final File subDirectory : subDirectories) {
      subDirectoriesNames.add(subDirectory.getName());
    }
    for (final String stringToSearch : searchList) {
      if (subDirectoriesNames.contains(stringToSearch)) {
        return stringToSearch;
      }
    }
    return null;
  }

  private FileUtil() {}
}
