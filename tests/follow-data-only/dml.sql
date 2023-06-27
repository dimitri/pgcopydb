INSERT INTO table_a (f1, f2)
     SELECT x,
            (array[E'test new\nline',
                   E'\\\\Client\\S$\\2023\\Dir1\\Test_Doc №2',
                   E'test \rreturn',
                   E'test ''single'' quote'])[(x - 1) % 4 + 1]
       FROM generate_series(:a,:b) as t(x);
