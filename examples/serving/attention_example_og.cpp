#include <torch/torch.h>
#include <math.h>
#include <iostream>

void single_attention(const torch::Tensor& q, const torch::Tensor& KV_cache, torch::Tensor& o, const size_t i, const size_t b)
{
    const auto dk_sqrt = sqrt(double(q.sizes()[0]));

    auto A = torch::zeros({int64_t(i)});

    for(size_t j = 0; j < i; ++j){
        A[j] = exp(torch::dot(q, KV_cache[b][0][j])/dk_sqrt);

        auto tmp = torch::zeros({1});
        for(size_t t = 0; t < i; ++t){
            tmp += exp(torch::dot(q, KV_cache[b][0][j])/dk_sqrt);
        }

        A[j] /= tmp[0];

        o[b][i] += A[j]*KV_cache[b][1][j];
    }
}

int main(int argc, char *argv[])
{
    torch::manual_seed(42);

    // Variables
    const size_t B = 1; // batch size
    const size_t n = 8; // max sequence length
    const size_t d = 4; // hidden state size (or number of possible tokens)

    // query, key, value matrices filled with random floats
    auto Wq = torch::rand({d,d});
    auto Wk = torch::rand({d,d});
    auto Wv = torch::rand({d,d});
    
    // KV cache
    auto KV_cache = torch::zeros({B, 2, n, d});
    
    // prompt + output lists
    auto o = torch::zeros({B, n, d});
    const size_t p_size = 3; // prompt size (this one will later be flexible)

    // creating prompts
    o.slice(1, 0, p_size) = torch::rand({B, p_size, d});

    // prompt phase
    for(size_t b = 0; b < B; ++b){
        for(size_t i = 0; i < p_size; ++i){
            KV_cache[b][0][i] = torch::matmul(Wk, o[b][i]);
            KV_cache[b][1][i] = torch::matmul(Wv, o[b][i]);
        }
    }

    // autoregressive phase
    for(size_t b = 0; b < B; ++b){
        for(size_t i = p_size; i < n; ++i){
            auto q            = torch::matmul(Wq, o[b][i]);
            KV_cache[b][0][i] = torch::matmul(Wk, o[b][i]);
            KV_cache[b][1][i] = torch::matmul(Wv, o[b][i]);

            single_attention(q, KV_cache, o, i, b);

            std::cout << "b: " << b << ", i: " << i << ", o: " << o << "\n";
        }
    }

    return 0;
}