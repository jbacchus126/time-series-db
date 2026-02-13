{
  "size" : 0,
  "query" : {
    "match_all" : {
      "boost" : 1.0
    }
  },
  "track_total_hits" : -1,
  "aggregations" : {
    "0" : {
      "filter" : {
        "match_all" : {
          "boost" : 1.0
        }
      },
      "aggregations" : {
        "0_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 1000000000,
            "max_timestamp" : 1001000000,
            "step" : 100000
          }
        }
      }
    },
    "0_coordinator" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "mockFetch",
            "values" : [
              1.0,
              2.0,
              3.0,
              4.0,
              5.0
            ],
            "tags" : {
              "name" : "test_series",
              "region" : "us-east"
            },
            "startTime" : 1000000000,
            "step" : 100000
          }
        ],
        "references" : {
          "0_unfold" : "0>0_unfold"
        },
        "inputReference" : "0_unfold"
      }
    }
  }
}
