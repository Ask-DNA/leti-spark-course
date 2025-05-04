namespace WarehouseAccountingApp.Domain.Models
{
    public class Pallet
    {
        private readonly List<Box> _boxes;
        public int ID { get; init; }
        public int Width { get; init; }
        public int Height { get; init; }
        public int Length { get; init; }
        public int Weight
        {
            get
            {
                int result = 30;
                foreach (Box b in _boxes)
                    result += b.Weight;
                return result;
            }
        }
        public int Volume
        {
            get
            {
                int result = Width * Height * Length;
                foreach (Box b in _boxes)
                    result += b.Volume;
                return result;
            }
        }
        public DateOnly? ExpirationDate
        {
            get
            {
                if (_boxes.Count == 0)
                    return null;
                return _boxes.Min(b => b.ExpirationDate);
            }
        }
        public IReadOnlyList<Box> Boxes { get => _boxes; }

        public Pallet(int id, int width, int height, int length, IEnumerable<Box> boxes)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(id, 0, nameof(id));
            ArgumentOutOfRangeException.ThrowIfLessThan(width, 1, nameof(width));
            ArgumentOutOfRangeException.ThrowIfLessThan(height, 1, nameof(height));
            ArgumentOutOfRangeException.ThrowIfLessThan(length, 1, nameof(length));
            ID = id;
            Width = width;
            Height = height;
            Length = length;
            foreach (Box box in boxes)
                if (box.Width > width || box.Length > length)
                    throw new ArgumentException("Invalid box size");
            _boxes = new(boxes);
        }
    }
}
