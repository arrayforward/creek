(module
  (import "env" "creek_get_metadata" (func $creek_get_metadata (param i32 i32) (result i32)))
  (import "env" "creek_set_metadata" (func $creek_set_metadata (param i32 i32 i32 i32)))
  (import "env" "creek_sleep" (func $creek_sleep (param i32)))
  (import "env" "creek_random" (func $creek_random (result i32)))
  (import "env" "creek_log" (func $creek_log (param i32 i32)))

  (memory (export "memory") 1)

  (data (i32.const 0) "ok")
  (data (i32.const 10) "wasm")
  (data (i32.const 20) "true")
  (data (i32.const 30) "mirror")
  (data (i32.const 50) "[wasm] on_request ok")
  (data (i32.const 80) "[wasm] on_response ok")

  (func (export "on_request") (param $svc i32) (param $method i32) (param $meta_ptr i32) (param $out_ptr i32)
    (i32.store8 (i32.add (local.get $out_ptr) (i32.const 0)) (i32.const 1))
    (call $creek_set_metadata (i32.const 0) (i32.const 2) (i32.const 20) (i32.const 4))
    (call $creek_log (i32.const 50) (i32.const 21))
  )

  (func (export "on_response") (param $status i32) (param $meta_ptr i32) (param $out_ptr i32)
    (call $creek_set_metadata (i32.const 30) (i32.const 6) (i32.const 20) (i32.const 4))
    (call $creek_log (i32.const 80) (i32.const 22))
  )
)
