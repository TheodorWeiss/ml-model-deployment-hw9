terraform {
  required_version = ">= 1.5.0"

  required_providers {
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

# Используется локальный провайдер (local_file) —
# не требует credentials и работает без облака.
# В production заменяется на yandex / aws / google провайдеры.
provider "local" {}
provider "random" {}
