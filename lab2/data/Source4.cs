namespace ASPAPP.DOM.MODELS
{
    public class MyClass
    {
        private readonly List<Box> _korobki;
        public int Id { get; init; }
        public int W { get; init; }
        public int H { get; init; }
        public int L { get; init; }
        public int Ves
        {
            get
            {
                int ves = 30;
                foreach (Box b in _korobki)
                    ves += b.Ves;
                return ves;
            }
        }
        public int Objem
        {
            get
            {
                int objem = W * H * L;
                foreach (Box b in _korobki)
                    objem += b.Objem;
                return objem;
            }
        }
        public DateOnly? DateOfExpiration
        {
            get
            {
                if (_korobki.Count == 0)
                    return null;
                return _korobki.Min(b => b.ExpirationDate);
            }
        }
        public IReadOnlyList<Box> Korobki { get => _korobki; }

        public MyClass(int id, int w, int h, int l, IEnumerable<Box> korobki)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(id, 0, nameof(id));
            ArgumentOutOfRangeException.ThrowIfLessThan(w, 1, nameof(w));
            ArgumentOutOfRangeException.ThrowIfLessThan(h, 1, nameof(h));
            ArgumentOutOfRangeException.ThrowIfLessThan(l, 1, nameof(l));
            Id = id;
            W = w;
            H = h;
            L = l;
            foreach (Box korobka in korobki)
                if (korobka.W > w || korobka.L > l)
                    throw new ArgumentException("ERROR!!!!!!");
            _korobki = new(korobki);
        }
    }
}
