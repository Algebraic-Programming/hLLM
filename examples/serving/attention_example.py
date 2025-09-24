import numpy as np
import scipy

np.random.seed(42)

np.set_printoptions(precision=2)

def single_attention(q: np.ndarray, K: np.ndarray, V: np.ndarray, o: np.ndarray, i: int, b: int) -> None:
    """_summary_

    Args:
        q (np.ndarray): query vector
        K (np.ndarray): Key cache matrix
        V (np.ndarray): Value cache matrix
        o (np.ndarray): Output vector (including the prompt)
        i (int): i-th index of the output
        b (int): batch index
    """
    dk_sqrt = len(q) ** 0.5

    A = np.zeros((i))

    for j in range(i):

        # A[j] = scipy.special.softmax(np.dot(q, K[b, j, :])/dk_sqrt)
        A[j] = np.exp(np.dot(q, K[b, j, :])/dk_sqrt)

        tmp = 0.0
        for t in range(i):
            tmp += np.exp(np.dot(q, K[b, t, :])/dk_sqrt)

        A[j] /= tmp
        
        o[b, i, :] += A[j]*V[b, j, :]



def main():
    B = 2   # batch size
    n = 8   # max sequence length
    d = 4   # hidden state size (or number of possible tokens)

    assert B < 3, "With one it is looking better"

    # query, key, value matrices filled with random floats
    Wq = np.random.rand(d,d)
    Wk = np.random.rand(d,d)
    Wv = np.random.rand(d,d)

    # KV cache
    K = np.zeros((B, n, d))
    V = np.zeros((B, n, d))

    # prompt + output lists
    o = np.zeros((B, n, d))
    p_size = 3 # prompt size

    # creating prompts
    for b in range(B):
        for i in range(p_size):
            o[b, i, :] = np.random.rand(d)

    # prompt phase
    for b in range(B):
        for i in range(p_size):
            K[b, i, :] = Wk @ o[b, i, :]
            V[b, i, :] = Wv @ o[b, i, :]

    # autoregressive phase
    for b in range(B):
        for i in range(p_size, n):
            # Computing the q,k,v of the given iteration (and storing KV cache)
            q          = Wq @ o[b, i, :]
            K[b, i, :] = Wk @ o[b, i, :]
            V[b, i, :] = Wv @ o[b, i, :]

            # single header attention of the given iteration for the output o_i
            single_attention(q, K, V, o, i, b)

            # print result
            print(f"b: {b}, i: {i}, o:\n{o[b, :, :]}")


if __name__ == "__main__":
    main()