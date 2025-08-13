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

public final class GraalVersionUtil {
  public static boolean isGraalVersionGreaterOrEqualThan(
      final String graalVersion, final int majorVersion, final int minorVersion) {
    try {
      final String[] versionSplit = graalVersion.split("\\.", -1);
      final Integer majorVersion0 = Integer.valueOf(versionSplit[0]);
      final Integer minorVersion0 = Integer.valueOf(versionSplit[1]);
      return majorVersion0 > majorVersion
          || (majorVersion0 == majorVersion && minorVersion0 >= minorVersion);
    } catch (NumberFormatException ignored) {
      return false;
    }
  }

  private GraalVersionUtil() {}
}
