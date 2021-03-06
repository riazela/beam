---
layout: section
title: "GroupByKey"
permalink: /documentation/transforms/python/aggregation/groupbykey/
section_menu: section-menu/documentation.html
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# GroupByKey
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.GroupByKey">
      <img src="https://beam.apache.org/images/logos/sdks/python.png" width="20px" height="20px"
           alt="Pydoc" />
     Pydoc
    </a>
</table>
<br>
Takes a keyed collection of elements and produces a collection
where each element consists of a key and all values associated with that key.

See more information in the [Beam Programming Guide]({{ site.baseurl }}/documentation/programming-guide/#groupbykey).

## Examples
See [BEAM-7390](https://issues.apache.org/jira/browse/BEAM-7390) for updates. 

## Related transforms 
* [CombineGlobally]({{ site.baseurl }}/documentation/transforms/python/aggregation/combineglobally) for combining all values associated with a key to a single result.
* [CoGroupByKey]({{ site.baseurl }}/documentation/transforms/python/aggregation/cogroupbykey) for multiple input collections.