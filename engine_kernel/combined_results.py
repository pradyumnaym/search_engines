from naive_retriever import get_result


def sorted_list_combination(list1: list[(int, float)], list2: list[(int, float)], alpha=0.5) -> list[(int, float)]:
    # final_score = alpha * score_1 + (1 - alpha) * score_2 + offset

    pointer_1 = 0
    pointer_2 = 0

    results = []
    seen_indices = {}
    result_index = 0

    while pointer_1 < len(list1) and pointer_2 < len(list2):
        index_1, score_1 = list1[pointer_1]
        index_2, score_2 = list2[pointer_2]

        # adding the score to an already seen index
        if index_1 in seen_indices:
            result_index_of_score = seen_indices[index_1]
            # the previous result contains a score of list 2 (since every list should contain one doc only once)
            previous_result = results[result_index_of_score]
            new_score = alpha * score_1 + (1 - alpha) * previous_result[1]
            results[result_index_of_score] = (previous_result[0], new_score)
            pointer_1 += 1
            continue
        elif index_2 in seen_indices:
            result_index_of_score = seen_indices[index_2]
            previous_result = results[result_index_of_score]
            new_score = alpha * previous_result[1] + (1 - alpha) * score_2
            results[result_index_of_score] = (previous_result[0], new_score)
            pointer_2 += 1
            continue

        if score_1 <= score_2:
            results.append((index_1, score_1))
            seen_indices[index_1] = result_index
            pointer_1 += 1
        else:
            results.append((index_2, score_2))
            seen_indices[index_2] = result_index
            pointer_2 += 1

    return results


def n_combined_urls(query: str, n: int, search_factor=5) -> list[(int, float)]:
    embedding_results = get_result(query, n * search_factor)

    return embedding_results[:n]


