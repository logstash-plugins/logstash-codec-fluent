## 3.4.3
  - Fix: reduce overhead of unpacking packforward-payloads by reusing a single instance [#32](https://github.com/logstash-plugins/logstash-codec-fluent/pull/32)

## 3.4.2
  - Fix: Convert LogStash::Timestamp values to iso-8601 to resolve crash issue with `msgpack` serialization [#30](https://github.com/logstash-plugins/logstash-codec-fluent/pull/30)

## 3.4.1
  - Fix: handle multiple PackForward-encoded messages in a single payload [#28](https://github.com/logstash-plugins/logstash-codec-fluent/pull/28)

## 3.4.0
  - Feat: added target configuration + event-factory support [#27](https://github.com/logstash-plugins/logstash-codec-fluent/pull/27)
  - Fix: decoding of time's nano-second precision 

## 3.3.0
  - Handle EventTime msgpack extension to handle nanosecond precision time and add its parameter [#18](https://github.com/logstash-plugins/logstash-codec-fluent/pull/18)

## 3.2.0
  - Encode tags as fluent forward protocol tags. Ref: https://github.com/logstash-plugins/logstash-codec-fluent/pull/21

## 3.1.5
  - Update gemspec summary

## 3.1.4
  - Constrain msgpack dependency to ~> 1.1 due to old versions not containing some format and type definitions
## 3.1.3
  - Fix some documentation issues

## 3.0.2
  - Relax constraint on logstash-core-plugin-api to >= 1.60 <= 2.99

## 3.0.1
  - Republish all the gems under jruby.
## 3.0.0
  - Update the plugin to the version 2.0 of the plugin api, this change is required for Logstash 5.0 compatibility. See https://github.com/elastic/logstash/issues/5141
# 2.0.4
  - Depend on logstash-core-plugin-api instead of logstash-core, removing the need to mass update plugins on major releases of logstash
# 2.0.3
  - New dependency requirements for logstash-core for the 5.0 release
## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully, 
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

