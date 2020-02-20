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
-- invalid_inline_task1.rg:23: inline tasks cannot have multiple return statements
-- task f(x : int) : int
--    ^

import "regent"

__demand(__inline)
task f(x : int) : int
  if x >= 0 then
    return x + 5
  else
    return x - 5
  end
end

task main()
  f(1)
end

regentlib.start(main)
