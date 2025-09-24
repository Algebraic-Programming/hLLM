#include <torch/torch.h>
#include <math.h>

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
