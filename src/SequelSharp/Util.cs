using System;
using System.Linq;
using System.Reflection;
using System.Data.Common;
using System.Collections.Generic;

namespace SequelSharp {

	/// <summary>Bag o Tricks</summary>
	public static class Util {

        public static IDictionary<string, object> ObjectToDictionary(this object anonymousType) {
			if (anonymousType == null) return null;
            var attr = BindingFlags.Public | BindingFlags.Instance;
            var dict = new Dictionary<string, object>();
            foreach (var property in anonymousType.GetType().GetProperties(attr))
                if (property.CanRead)
                    dict.Add(property.Name, property.GetValue(anonymousType, null));
            return dict;
        } 
	}
}
