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
                throw new InvalidFilterConfigurationException(OptionsManager.Errors);

            ExpressionBuilder.Refresh();

            foreach (FilterOption option in OptionsManager.FilterOptions)
                ExpressionBuilder.AddFilterOption(option);

            return ExpressionBuilder.Create();
        }

        public void Method1()
        {
            string pattern = @"^(?("")(""[^""]+?""@)|(([0-9a-z]((\.(?!\.))|[-!#\$%&'\*\+/=\?\^`\{\}\|~\w])*)(?<=[0-9a-z])@))" +
                @"(?(\[)(\[(\d{1,3}\.){3}\d{1,3}\])|(([0-9a-z][-\w]*[0-9a-z]*\.)+[a-z0-9]{2,17}))$";
            var data = new string[]
            {
    "tom@gmail.com",
    "+12345678999",
    "bob@yahoo.com",
    "+13435465566",
    "sam@yandex.ru",
    "+43743989393"
            };

            Console.WriteLine("Email List");
            for (int i = 0; i < data.Length; i++)
            {
                if (Regex.IsMatch(data[i], pattern, RegexOptions.IgnoreCase))
                {
                    Console.WriteLine(data[i]);
                }
            }
        }

        public Expression<Func<T, bool>> AsExpression()
        {
            return Build();
        }

        public Func<T, bool> AsDelegate()
        {
            return Build().Compile();
        }

        public void Method2()
        {
            Console.WriteLine("Main Starts");
            // создаем задачу
            Task task1 = new Task(() =>
            {
                Console.WriteLine("Task Starts");
                Thread.Sleep(1000);     // задержка на 1 секунду - имитация долгой работы
                Console.WriteLine("Task Ends");
            });
            task1.Start();  // запускаем задачу
            task1.Wait();   // ожидаем выполнения задачи
            Console.WriteLine("Main Ends");
        }

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
