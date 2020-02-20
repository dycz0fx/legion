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
-- invalid_import_ispace3.rg:23: cannot import a handle that is already imported

import "regent"

task main()
  var is = ispace(int1d, 10)
  var raw_is = __raw(is)
  var is2 = __import_ispace(int1d, raw_is)
end

regentlib.start(main)
