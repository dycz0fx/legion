-- Copyright 2021 Stanford University
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
-- optimize_index_launch_list4.rg:70: loop optimization failed: body is not a function call
--     var x = f(p_disjoint[i])
--       ^

import "regent"

-- This tests the various loop optimizations supported by the
-- compiler.

local c = regentlib.c

terra e(x : int) : int
  return 3
end

task f(r : region(int)) : int
where reads(r) do
  return 5
end

task f2(r : region(int), s : region(int)) : int
where reads(r, s) do
  return 5
end

task g(r : region(int)) : int
where reads(r), writes(r) do
  return 5
end

task g2(r : region(int), s : region(int)) : int
where reads(r, s), writes(r, s) do
  return 5
end

task main()
  var n = 5
  var cs = ispace(int1d, n)
  var r = region(ispace(ptr, n), int)
  var rc = c.legion_coloring_create()
  for i = 0, n do
    c.legion_coloring_ensure_color(rc, i)
  end
  var p_disjoint = partition(disjoint, r, rc)
  var p_aliased = partition(aliased, r, rc)
  var r0 = p_disjoint[0]
  var r1 = p_disjoint[1]
  var p0_disjoint = partition(disjoint, r0, rc)
  var p1_disjoint = partition(disjoint, r1, rc)
  c.legion_coloring_destroy(rc)

  -- not optimized: body is not a bare function call
  __demand(__index_launch)
  for i in cs do
    var x = f(p_disjoint[i])
  end
end
regentlib.start(main)
