-- Copyright 2020 Stanford University
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- fails-with:
-- check_parallelizable_parallelize4.rg:27: partition driven auto-parallelization failed: found a region access outside parallelizable loops
--     s += @e
--          ^

import "regent"

__demand(__parallel)
task f(r : region(ispace(int1d), int))
where reads writes(r) do
  var s = 0
  for e in r do
    s += @e
    r[e] = r[e + 1]
  end
  return s
end
