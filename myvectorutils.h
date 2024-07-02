
/* A generic key-value map for options */
typedef unordered_map<string, string> OptionsMap;

/* Trim leading & trailing spaces */
inline string lrtrim(const string &str)
{
 string ret =
   regex_replace(str, regex("^ +| +$|( ) +"), "$1");
 
 return ret;
}

/* Split a string at comma and return ordered list */
inline void split(const string &str, vector<string> &out)
{
  static const regex re("[^,]+");

  sregex_iterator current(str.begin(), str.end(), re);
  sregex_iterator end;
  while (current != end) {
    string val = lrtrim((*current).str());
    out.push_back(val);
    current++;
  }
}

/* Helper class to manage vector index options as k-v map.
 * e.g type=HNSW,dim=1536,size=1000000,M=64,ef=100
 */
class MyVectorOptions {
  private:
    /* Returns true on success, false on format error */
    bool parseKV(const char *line)
    {
      /* e.g options list-MYVECTOR(type=hnsw,dim=50,size=4000000,M=64,ef=100) */
      const char *ptr1 = line;
      if (strchr(line,'|')) { // start marker
        ptr1 = strchr(line,'|');
        ptr1++;
      }
      string          sline(ptr1);
      vector<string>  listoptions;

      split(sline, listoptions);

      for (auto s : listoptions) {
        size_t eq = s.find_first_of('=');
        if (eq == string::npos) return false;

        string k = s.substr(0, eq);
        string v = s.substr(eq+1);

        k = lrtrim(k);
        v = lrtrim(v);

        if (!k.length() || !v.length()) return false;

        setOption(k, v);
      }

      return true;
    } /* parseKV */

  public:
    MyVectorOptions(const string &options) {
      m_valid = false;
      if (parseKV(options.c_str())) m_valid = true;
    }

    bool        isValid() const { return m_valid; }
    void        setOption(const string &name, const string &val)
      { m_options[name] = val; }
    string getOption(const string &name)
      { string ret = "";
        if (m_options.find(name) != m_options.end()) ret = m_options[name];
        return ret;
      }
           
  private:
    OptionsMap m_options;
    bool       m_valid;
};

#if TODO
/* Compare 2 binlog coordinates */
int binlogPositionCompare(const std::string &file1, size_t pos1,
                          const std::string &file2, size_t pos2) {
  if ((file2 == file1 && pos2 > pos1) || (file2 > file1))
    return 1;
  else if (file2 == file1 && pos2 == pos1)
    return 0;
  else
    return -1;
}
#endif
