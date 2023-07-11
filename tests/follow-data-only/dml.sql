INSERT INTO table_a (f1, f2)
     SELECT x,
            (array[E'test new\nline',
                   E'\\\\Client\\S$\\2023\\Dir1\\Test_Doc №2',
                   E'test \rreturn',
                   E'test ''single'' quote'])[(x - 1) % 4 + 1]
       FROM generate_series(:a,:b) as t(x);

INSERT INTO table_b(f1, f2)
    SELECT x, (regexp_split_to_array(lorem.ipsum, '[ ,.]'))[x:x+5]
      FROM (values('Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.')) as lorem(ipsum),
           generate_series(:a,:b) as t(x);
