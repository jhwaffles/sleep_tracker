graph TD
    subgraph Raw Sources
        A[raw_sleep]
        B[raw_daily_sleep]
    end

    subgraph Staging Models
        C[stg_sleep]
        D[stg_daily_sleep]
    end

    subgraph Intermediate Models
        E[int_sleep_timeseries]
    end
    
    subgraph Mart Models
        F[fct_daily_sleep]
        G[fct_sleep_epochs]
    end

    subgraph ML Models
        H[ml_features_simple]
        I[ml_features_pca]
        J[fct_sleep_disruption_events]
    end

    %% Define Dependencies
    A --> C
    B --> D
    
    C --> E
    C --> F
    D --> F
    
    E --> G
    
    G --> H
    G --> I

    H --> J