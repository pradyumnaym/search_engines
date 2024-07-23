from engine_kernel.naive_retriever import get_result as llm_retrieve
from engine_kernel.bm25 import retrieve as bm25_retrieve


def insert_into_sorted_list(list: list[(int, float)], new_element: (int, float)) -> list[(int, float)]:
    for i in range(len(list)):
        _, score = list[i]
        if score > new_element[1]:
            list.insert(i, new_element)
            break

    return list


def weighted_combination(value1, value2, alpha):
    return (value1 * alpha) + (value2 * (1 - alpha))


def rank_combination_old(list1: list[(int, float)], list2: list[(int, float)], alpha=0.5,
                     lone_penalty=10) -> list[(int, float)]:

    pointer = 0

    result = []
    # first interleave the lists, without regard to double docs
    while pointer < len(list1) and pointer < len(list2):
        # originally the algorithm was meant to combine the scores of the two results.
        # But the scores are not normalized to be used together.
        # Instead, we use the rank (or the pointer) of the document.

        index_1, _ = list1[pointer]
        index_2, _ = list2[pointer]

        if index_1 == index_2:
            # shortcut if one document is at the same rank in both lists
            result.append((index_1, weighted_combination(pointer, pointer, alpha)))
        else:
            # a document that isn't mentioned in both lists in penalized
            result.append((index_1, pointer + lone_penalty))

            # by convention the second list is disadvantaged and inserted after the result of list1,
            # although they have the same rank
            result.append((index_2, pointer + lone_penalty))

        pointer += 1

    # combined list is created
    # now the list is reviewed and docs that are referenced twice are combined.
    seen_indices = {}
    to_delete = []

    for result_index in range(len(result)):
        doc_index, score = result[result_index]

        # if the doc was already seen it is combined with the already seen score
        if doc_index in seen_indices:
            second_index = seen_indices[doc_index]
            _, second_score = result[result_index]

            to_delete.append(result_index)
            to_delete.append(second_index)

            # the lone penalty is subtracted since the doc is no longer referenced by only one list
            new_score = weighted_combination(score - lone_penalty, second_score - lone_penalty, alpha)
            result = insert_into_sorted_list(result, (doc_index, new_score))
        else:
            # if the doc is new it is only added to the seen indices
            seen_indices[doc_index] = result_index

    to_delete.sort()
    deletion_counter = 0
    for index in to_delete:
        del result[index - deletion_counter]

    return result

def rank_combination(list1: list[(int, float)], list2: list[(int, float)], alpha=0.5,
                         lone_penalty=10) -> list[(int, float)]:

    double_docs = set(map(lambda x: x[0], list1)).intersection(map(lambda x: x[1], list2))
    seen_indices_1 = {}
    seen_indices_2 = {}

    pointer = 0

    result = []
    # first interleave the lists, without double docs
    while pointer < len(list1) and pointer < len(list2):
        # originally the algorithm was meant to combine the scores of the two results.
        # But the scores are not normalized to be used together.
        # Instead, we use the rank (or the pointer) of the document.

        index_1, _ = list1[pointer]
        index_2, _ = list2[pointer]
        if index_1 not in double_docs:
            # either adding the doc to the result because it won't occur twice
            result.append((index_1, pointer + lone_penalty))
        else:
            # or adding it to the list of seen double docs of list 1
            seen_indices_1[index_1] = pointer

        if index_2 not in double_docs:
            result.append((index_2, pointer + lone_penalty))
        else:
            seen_indices_2[index_2] = pointer

        pointer += 1

    for double_doc_index in double_docs:
        rank_1 = seen_indices_1[double_doc_index]
        rank_2 = seen_indices_2[double_doc_index]

        new_score = weighted_combination(rank_1, rank_2, alpha)
        result = insert_into_sorted_list(result, (double_doc_index, new_score))

    return result


def n_combined_urls(query: str, n: int) -> list[(int, float)]:
    bm25_results = bm25_retrieve(query, n=n)
    llm_results = llm_retrieve(query, n)

    combined_results = rank_combination(llm_results, bm25_results, alpha=0.7)

    return combined_results[:n]


