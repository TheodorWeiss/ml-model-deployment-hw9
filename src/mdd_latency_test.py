"""Metrics Driven Development: статистический тест задержки инвентарного прогноза.

Метрика: p95-задержка вызова inventory forecast (мс).

Сценарий:
  - reference_latency.csv — исторические данные (базовый уровень ~150 мс p95)
  - new_latency.csv       — новые данные после изменения (деградация ~800 мс p95)

Тест Манна–Уитни (непараметрический, не предполагает нормальность).
Решение принимается по двум критериям:
  1. p-value < alpha (статистическая значимость)
  2. delta_p95 > practical_slo_delta (практическая значимость)
"""

import json
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from scipy import stats
from datetime import datetime

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
REPORTS_DIR = REPO_ROOT / "reports"

# SLO-пороги задержки (мс)
LATENCY_SLO_NORMAL_P95 = 300      # нормальный порог p95
LATENCY_SLO_CRITICAL_P95 = 1000   # критический порог p95
ALPHA = 0.05                        # уровень значимости


def generate_reference_data(n: int = 200, seed: int = 42) -> pd.DataFrame:
    """Синтетическая референсная выборка: p95 ~ 150 мс (нормальная работа)."""
    rng = np.random.default_rng(seed)
    latency = rng.gamma(shape=3.0, scale=40.0, size=n)  # mean~120, p95~200
    latency = np.clip(latency, 20, 400)
    return pd.DataFrame({"latency_ms": np.round(latency, 2)})


def generate_new_data(n: int = 200, seed: int = 99) -> pd.DataFrame:
    """Синтетическая новая выборка: деградация ~3-sigma — p95 > 800 мс."""
    rng = np.random.default_rng(seed)
    latency = rng.gamma(shape=4.0, scale=180.0, size=n)  # mean~720, p95~1100
    latency = np.clip(latency, 100, 3000)
    return pd.DataFrame({"latency_ms": np.round(latency, 2)})


def load_or_generate() -> tuple[pd.DataFrame, pd.DataFrame]:
    ref_path = DATA_DIR / "reference_latency.csv"
    new_path = DATA_DIR / "new_latency.csv"

    if ref_path.exists():
        ref_df = pd.read_csv(ref_path)
    else:
        ref_df = generate_reference_data()
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        ref_df.to_csv(ref_path, index=False)
        print(f"[mdd] Сгенерированы референсные данные → {ref_path}")

    if new_path.exists():
        new_df = pd.read_csv(new_path)
    else:
        new_df = generate_new_data()
        new_df.to_csv(new_path, index=False)
        print(f"[mdd] Сгенерированы новые данные → {new_path}")

    return ref_df, new_df


def run_test(ref_df: pd.DataFrame, new_df: pd.DataFrame) -> dict:
    ref = ref_df["latency_ms"].values
    new = new_df["latency_ms"].values

    # Дескриптивная статистика
    ref_p95 = float(np.percentile(ref, 95))
    new_p95 = float(np.percentile(new, 95))
    ref_mean = float(np.mean(ref))
    new_mean = float(np.mean(new))

    # Тест Манна–Уитни (двусторонний)
    stat, p_value = stats.mannwhitneyu(ref, new, alternative="two-sided")

    delta_p95 = new_p95 - ref_p95
    is_significant = p_value < ALPHA
    exceeds_slo = new_p95 > LATENCY_SLO_NORMAL_P95

    if is_significant and exceeds_slo:
        decision = "ДЕЙСТВОВАТЬ"
        action = (
            "Деградация задержки статистически значима и превышает SLO. "
            "Рекомендуется: добавить кеш перед чтением истории остатков; "
            "предрасчёт лаговых признаков в батче; "
            "удерживать предыдущую стабильную модель до устранения узкого места."
        )
        adl_conclusion = "reject"
    elif is_significant and not exceeds_slo:
        decision = "МОНИТОРИТЬ"
        action = (
            "Изменение статистически значимо, но p95 ещё ниже порога SLO. "
            "Усилить мониторинг, пересмотреть решение при следующем батче."
        )
        adl_conclusion = "monitor"
    else:
        decision = "НИЧЕГО НЕ МЕНЯТЬ"
        action = (
            "Нулевая гипотеза не отвергается. Разница не значима статистически. "
            "Фиксируем риск дальнейшего роста задержки, оставляем систему без изменений."
        )
        adl_conclusion = "accept_null"

    return {
        "timestamp": datetime.now().isoformat(),
        "n_ref": len(ref),
        "n_new": len(new),
        "ref_mean_ms": round(ref_mean, 2),
        "new_mean_ms": round(new_mean, 2),
        "ref_p95_ms": round(ref_p95, 2),
        "new_p95_ms": round(new_p95, 2),
        "delta_p95_ms": round(delta_p95, 2),
        "mann_whitney_stat": round(float(stat), 2),
        "p_value": round(float(p_value), 6),
        "alpha": ALPHA,
        "is_significant": is_significant,
        "slo_normal_p95_ms": LATENCY_SLO_NORMAL_P95,
        "slo_critical_p95_ms": LATENCY_SLO_CRITICAL_P95,
        "exceeds_slo": exceeds_slo,
        "decision": decision,
        "action": action,
        "adl_conclusion": adl_conclusion,
    }


def write_report(result: dict) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / "mdd_test_result.md"

    sig_mark = "✅ Да" if result["is_significant"] else "❌ Нет"
    slo_mark = "⚠️ Превышен" if result["exceeds_slo"] else "✅ В норме"

    content = f"""# MDD: Тест задержки инвентарного прогноза

**Время:** {result['timestamp']}
**Метрика:** p95-задержка вызова inventory forecast (мс)

## Описательная статистика

| | Референс | Новые данные | Дельта |
|---|---|---|---|
| N | {result['n_ref']} | {result['n_new']} | — |
| Mean (мс) | {result['ref_mean_ms']} | {result['new_mean_ms']} | +{result['new_mean_ms'] - result['ref_mean_ms']:.1f} |
| p95 (мс) | {result['ref_p95_ms']} | {result['new_p95_ms']} | +{result['delta_p95_ms']:.1f} |

## Статистический тест (Манн–Уитни)

| Параметр | Значение |
|---|---|
| Статистика U | {result['mann_whitney_stat']} |
| p-value | {result['p_value']} |
| Уровень значимости α | {result['alpha']} |
| Статистически значимо? | {sig_mark} |

## SLO-порог

| Порог | Значение | Статус |
|---|---|---|
| Нормальный SLO p95 | {result['slo_normal_p95_ms']} мс | {slo_mark} |
| Критический SLO p95 | {result['slo_critical_p95_ms']} мс | — |

## Решение

**{result['decision']}**

{result['action']}

---
*Отчёт сгенерирован автоматически src/mdd_latency_test.py*
"""
    path.write_text(content, encoding="utf-8")
    print(f"[mdd] Отчёт → {path}")


def plot_distribution(ref_df: pd.DataFrame, new_df: pd.DataFrame, result: dict) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle("MDD: Распределение задержки inventory forecast", fontsize=13)

    # Гистограммы
    ax = axes[0]
    ax.hist(ref_df["latency_ms"], bins=30, alpha=0.6, label="Референс", color="steelblue")
    ax.hist(new_df["latency_ms"], bins=30, alpha=0.6, label="Новые данные", color="tomato")
    ax.axvline(result["slo_normal_p95_ms"], color="orange", linestyle="--", label=f"SLO p95={result['slo_normal_p95_ms']}мс")
    ax.axvline(result["slo_critical_p95_ms"], color="red", linestyle="--", label=f"Critical={result['slo_critical_p95_ms']}мс")
    ax.set_xlabel("Задержка (мс)")
    ax.set_ylabel("Количество")
    ax.legend(fontsize=8)
    ax.set_title("Гистограмма")

    # Боксплот
    ax2 = axes[1]
    ax2.boxplot(
        [ref_df["latency_ms"].values, new_df["latency_ms"].values],
        tick_labels=["Референс", "Новые данные"],
        patch_artist=True,
        boxprops=dict(facecolor="steelblue", alpha=0.6),
    )
    ax2.axhline(result["slo_normal_p95_ms"], color="orange", linestyle="--", label=f"SLO={result['slo_normal_p95_ms']}мс")
    ax2.set_ylabel("Задержка (мс)")
    ax2.legend(fontsize=8)
    ax2.set_title(f"Боксплот  p-value={result['p_value']}")

    plt.tight_layout()
    out = REPORTS_DIR / "mdd_latency_distribution.png"
    plt.savefig(out, dpi=120)
    plt.close()
    print(f"[mdd] График → {out}")


def main() -> dict:
    ref_df, new_df = load_or_generate()
    result = run_test(ref_df, new_df)
    write_report(result)
    try:
        plot_distribution(ref_df, new_df, result)
    except Exception as e:
        print(f"[mdd] Не удалось построить график: {e}")
    print(f"\n[mdd] РЕШЕНИЕ: {result['decision']}")
    print(f"[mdd] p-value={result['p_value']}, ref_p95={result['ref_p95_ms']}мс, new_p95={result['new_p95_ms']}мс")
    return result


if __name__ == "__main__":
    main()
