def get_distinct_subscriptions_aggr_pipeline(sz=5):
    # pipeline = [
    #     {
    #         '$match': {'subscriptions': {'$exists': True, '$size': sz}}
    #         # "$match": {"subscriptions" + "." + str(sz): {"$exists": True}}
    #     },
    #     {
    #         "$addFields": {
    #             "unique_subscriptions": {
    #                 "$reduce": {
    #                     "input": "$subscriptions",
    #                     "initialValue": [],
    #                     "in": {
    #                         "$concatArrays": [
    #                             "$$value",
    #                             {
    #                                 "$cond": [
    #                                     {
    #                                         "$and": [
    #                                             {
    #                                                 "$in": [
    #                                                     "$$this.productCode",
    #                                                     "$$value.productCode"
    #                                                 ]
    #                                             },
    #                                             {
    #                                                 "$in": [
    #                                                     "$$this.status",
    #                                                     "$$value.status"
    #                                                     # ["ACTIVE"]
    #                                                 ]
    #                                             },
    #                                             {
    #                                                 "$in": [
    #                                                     "$$this.type",
    #                                                     "$$value.type"
    #                                                 ]
    #                                             }
    #                                         ]
    #                                     },
    #                                     [],
    #                                     [
    #                                         "$$this"
    #                                     ]
    #                                 ]
    #                             }
    #                         ]
    #                     }
    #                 }
    #             }
    #         }
    #     },
    #     # {
    #     #   "$addFields": {
    #     #         "duplicate_subscriptions": {
    #     #             "$filter": {
    #     #                 "input": "$subscriptions",
    #     #                 "as": "a",
    #     #                 "cond": {
    #     #                     "$not": {
    #     #                         "$in": [
    #     #                             "$$a",
    #     #                             "$unique_subscriptions"
    #     #                         ]
    #     #                     }
    #     #                 }
    #     #             }
    #     #         }
    #     #     }
    #     # },
    #     {
    #         "$addFields": {
    #             "duplicate_subscriptions": {
    #                 "$setDifference": ["$subscriptions", "$unique_subscriptions"]
    #             }
    #         }
    #     },
    #     {
    #         "$match": {"duplicate_subscriptions.0": {"$exists": True}}
    #     }
    # ]

    pipeline = [
        {
            # '$match': {'subscriptions': {'$exists': True, '$size': sz}}
            "$match": {"subscriptions" + "." + str(sz): {"$exists": True}}
        },
        {
            '$addFields': {
                'unique_subscriptions': {
                    '$reduce': {
                        'input': '$subscriptions',
                        'initialValue': [],
                        'in': {
                            '$cond': {
                                'if': {
                                    '$and': [
                                        {
                                            '$in': [
                                                '$$this.productCode', '$$value.productCode'
                                            ]
                                        }, {
                                            '$in': [
                                                '$$this.status', '$$value.status'
                                            ]
                                        }, {
                                            '$in': [
                                                '$$this.type', '$$value.type'
                                            ]
                                        }
                                    ]
                                },
                                'then': '$$value',
                                'else': {
                                    '$concatArrays': [
                                        '$$value',
                                        ['$$this']
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        }
    ]

    return pipeline
