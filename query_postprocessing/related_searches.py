from transformers import AutoTokenizer, AutoModelForCausalLM

KEEP_MODEL_IN_GPU = True
huggingface_token = "<Token>"

def generate_related_queries_prompt(query):
    """
    Create a prompt that can be used to generated related queries. 

    Argument
    --------
    query: str, a query text

    Returns
    -------
    str, the prompt asking an LLM .

    """

    return f"""

I have a search engine where a user has entered the following query about the city of Tübingen: 

{query}

Please give me the top 5 related queries related to this query. Please print the related queries separated by a ';' (semicolon).
Please keep the related searches diverse, without redundancies, and do not include any thing else (not even numbering).
The related queries are:"""


if KEEP_MODEL_IN_GPU:
    preloaded_model = AutoModelForCausalLM.from_pretrained(
        "mistralai/Mistral-7B-Instruct-v0.3", device_map="cuda", load_in_4bit=True,
        token=huggingface_token
    )


def get_related_searches(query_list):
    """
    Get related searches for a list of queries.

    Argument
    --------
    query_list: list of str, a list of queries

    Returns
    -------
    list of list of str, a list of related searches for each query
    """
    prompts = list(map(generate_related_queries_prompt, query_list))

    if KEEP_MODEL_IN_GPU:
        model = preloaded_model
    else:
        model = AutoModelForCausalLM.from_pretrained(
            "mistralai/Mistral-7B-Instruct-v0.3", device_map="cuda", load_in_4bit=True,
            token=huggingface_token
        )


    tokenizer = AutoTokenizer.from_pretrained("mistralai/Mistral-7B-Instruct-v0.3",
                                              padding_side="left", token=huggingface_token)
    tokenizer.pad_token = tokenizer.eos_token  # Most LLMs don't have a pad token by default

    model_inputs = tokenizer(
        prompts, return_tensors="pt", padding=True
    ).to("cuda")
    generated_ids = model.generate(max_new_tokens = 50, **model_inputs)

    if not KEEP_MODEL_IN_GPU:
        del model

    outputs = tokenizer.batch_decode(generated_ids[:, model_inputs.input_ids.shape[1]:], skip_special_tokens=True)
    outputs = [
        [query.strip() for query in output.split(";")]
        for output in outputs
    ]

    return outputs
        
if __name__ == '__main__':

    ####### Related searches LLM call
    print(get_related_searches(['food and drinks', 'university library']))

