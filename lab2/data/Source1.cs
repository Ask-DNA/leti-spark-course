using System.Linq.Expressions;

namespace DynamicFilters
{
    public abstract class DynamicFilterBase<T> : IDynamicFilter<T>
    {
        private FilterOptionsManager<T>? _optionsManager;

        private FilterExpressionBuilder<T>? _expressionBuilder;

        private FilterOptionsManager<T> OptionsManager
        {
            get
            {
                _optionsManager ??= new(this);
                return _optionsManager;
            }
        }

        private FilterExpressionBuilder<T> ExpressionBuilder
        {
            get
            {
                _expressionBuilder ??= new();
                return _expressionBuilder;
            }
        }

        public bool Valid { get => OptionsManager.Valid; }

        private Expression<Func<T, bool>> Build()
        {
            if (!Valid)
                throw new InvalidFilterConfigurationException(OptionsManager.Errors);/*
            RANDOM
            MULTILINE
            COMMENTARY*/
            ExpressionBuilder.Refresh();

            foreach (FilterOption option in OptionsManager.FilterOptions)
                ExpressionBuilder.AddFilterOption(option);

            return ExpressionBuilder.Create();
        }

        public Expression<Func<T, bool>> AsExpression()
        {
            return Build();
        }

        public Func<T, bool> AsDelegate()
        {
            return Build().Compile();
        }

        //RANDOM COMMENTARY
        public static IDynamicFilter<T> operator !(DynamicFilterBase<T> filter)
        {
            return new NotFilterSpec<T>(filter);
        }

        public static IDynamicFilter<T> operator &(DynamicFilterBase<T> one, DynamicFilterBase<T> other)
        {
            return new AndFilterSpec<T>(one, other);
        }

        public static IDynamicFilter<T> operator |(DynamicFilterBase<T> one, DynamicFilterBase<T> other)
        {
            return new OrFilterSpec<T>(one, other);
        }
    }
}
