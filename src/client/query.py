def get_distinct_subscriptions_aggr_pipeline(sz=5):
    pipeline = [
            {
                # '$match': {'subscriptions': {'$exists': True, '$size': sz}}
                 "$match": {"subscriptions" + "." + str(sz) : {"$exists": True}}
            },
            {
                "$addFields": {
                    "distinct": {
                        "$reduce": {
                            "input": "$subscriptions",
                            "initialValue": [],
                            "in": {
                                "$concatArrays": [
                                    "$$value",
                                    {
                                        "$cond": [
                                            {
                                                "$and": [
                                                    {
                                                        "$in": [
                                                            "$$this.productCode",
                                                            "$$value.productCode"
                                                        ]
                                                    },
                                                    {
                                                        "$in": [
                                                            "$$this.status",
                                                            "$$value.status"
                                                        ]
                                                    },
                                                    {
                                                        "$in": [
                                                            "$$this.type",
                                                            "$$value.type"
                                                        ]
                                                    }
                                                ]
                                            },
                                            [],
                                            [
                                                "$$this"
                                            ]
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            },
            {
                "$addFields": {
                    "to_delete": { "$setDifference": ["$subscriptions", "$distinct"] }
                }
            }
            #,
            # {
            #     "$match": {"to_delete.0": {"$exists": True}}
            # },
            # {
            #     "$addFields": {
            #         "subscriptions":  "$distinct"
            #     }
            # },
            # {
            #     "$unset": ["to_delete", "distinct"]
            # },
        ]

    return pipeline