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
-- undefined_field_access_region1.rg:26: no field 'impl' in type region(int32)
--   r.impl.tree_id = 5
--    ^

import "regent"

task main()
  var r = region(ispace(ptr, 5), int)
  -- This field actually does exist, but we don't want people digging
  -- around the internals of regions.
  r.impl.tree_id = 5
end
regentlib.start(main)
