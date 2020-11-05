from pyspark.accumulators import AccumulatorParam


# pyspark accumulator for error bucket summarization
class ListErrorAccumulator(AccumulatorParam):
    def zero(self, value):
        return {}

    def addInPlace(self, final_dict, new_dict):
        for key in new_dict:
            # key is the column name
            # check if the accumulator already has an entry for the column
            if final_dict.get(key):
                # Get the existing error summary for this field/column/key
                val = final_dict.get(key)
                val.append(new_dict.get(key)) if len(val) < 10 else ''
                # push the result back to accumulator/final_dict
                final_dict[key] = val
            else:
                # accumulator doesn't have an entry for the key => error on a new column
                final_dict[key] = [new_dict.get(key)]
        return final_dict
